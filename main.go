package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"github.com/gorilla/websocket"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/notnil/chess"
	"google.golang.org/api/option"
)

type Game struct {
	Code    string
	Game    *chess.Game
	Clients map[*websocket.Conn]bool
	Mu      sync.Mutex

	// Datos añadidos para poder reanudar partidas.
	Blancas     string      // jugadorId del jugador de blancas
	Negras      string      // jugadorId del jugador de negras
	Movimientos []string    // historial de movimientos en formato UCI (e2e4, g1f3...)
	Terminada   bool        // true cuando hay jaque mate, tablas o abandono
	Resultado   string      // "Jaque mate", "Tablas" o "Abandono"
	Abandona    chess.Color // color del jugador que abandonó
	EnJaque     bool        // true si el jugador que tiene el turno está en jaque

	// Identificadores de notificaciones (Firebase) de cada jugador.
	TokenBlancas string
	TokenNegras  string
}

var games = struct {
	sync.RWMutex
	m map[string]*Game
}{
	m: make(map[string]*Game),
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// Letras permitidas en el código de partida (sin I ni O, que se confunden con 1 y 0).
const letrasCodigo = "ABCDEFGHJKLMNPQRSTUVWXYZ"

// code genera un código de partida: una letra y tres cifras (por ejemplo, F638).
func code() string {
	b := make([]byte, 3)
	_, _ = rand.Read(b)

	letra := letrasCodigo[int(b[0])%len(letrasCodigo)]
	numero := (int(b[1])<<8 | int(b[2])) % 1000

	return fmt.Sprintf("%c%03d", letra, numero)
}

// db es nil si no hay DATABASE_URL (en ese caso solo se usa la memoria).
var db *sql.DB

// limpiarURL quita parámetros de la cadena de Neon que el controlador no necesita.
func limpiarURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}

	q := u.Query()
	q.Del("channel_binding")
	u.RawQuery = q.Encode()

	return u.String()
}

func iniciarBD(cadena string) error {
	var err error

	db, err = sql.Open("pgx", limpiarURL(cadena))
	if err != nil {
		return err
	}

	// Neon suspende el cálculo tras 5 minutos sin uso: se cierran antes
	// las conexiones inactivas para no reutilizar una que ya no sirve.
	db.SetConnMaxIdleTime(3 * time.Minute)
	db.SetConnMaxLifetime(30 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err = db.PingContext(ctx); err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS partidas (
	codigo TEXT PRIMARY KEY,
	blancas TEXT NOT NULL DEFAULT '',
	negras TEXT NOT NULL DEFAULT '',
	movimientos TEXT NOT NULL DEFAULT '',
	terminada BOOLEAN NOT NULL DEFAULT FALSE,
	resultado TEXT NOT NULL DEFAULT '',
	abandona INTEGER NOT NULL DEFAULT 0,
	actualizada TIMESTAMPTZ NOT NULL DEFAULT now()
)`)
	if err != nil {
		return err
	}

	for _, columna := range []string{"token_blancas", "token_negras"} {
		_, err = db.ExecContext(
			ctx,
			"ALTER TABLE partidas ADD COLUMN IF NOT EXISTS "+columna+" TEXT NOT NULL DEFAULT ''",
		)
		if err != nil {
			return err
		}
	}

	return nil
}

// guardarPartida escribe el estado de la partida en la base de datos.
// Debe llamarse con g.Mu bloqueado (o antes de publicar la partida),
// para que los guardados de una misma partida se hagan en orden.
// Si falla, se reintenta una vez.
func guardarPartida(g *Game) {
	if db == nil {
		return
	}

	var err error

	for intento := 1; intento <= 2; intento++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		_, err = db.ExecContext(ctx, `INSERT INTO partidas
	(codigo, blancas, negras, movimientos, terminada, resultado, abandona, token_blancas, token_negras, actualizada)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now())
ON CONFLICT (codigo) DO UPDATE SET
	blancas = EXCLUDED.blancas,
	negras = EXCLUDED.negras,
	movimientos = EXCLUDED.movimientos,
	terminada = EXCLUDED.terminada,
	resultado = EXCLUDED.resultado,
	abandona = EXCLUDED.abandona,
	token_blancas = EXCLUDED.token_blancas,
	token_negras = EXCLUDED.token_negras,
	actualizada = now()`,
			g.Code,
			g.Blancas,
			g.Negras,
			strings.Join(g.Movimientos, ","),
			g.Terminada,
			g.Resultado,
			int(g.Abandona),
			g.TokenBlancas,
			g.TokenNegras,
		)

		cancel()

		if err == nil {
			return
		}

		log.Printf("Error guardando partida %s (intento %d): %v", g.Code, intento, err)
	}
}

// Días que se conserva una partida terminada antes de borrarla.
// Así un jugador que cierre la app puede ver el resultado al volver.
const diasConservarTerminadas = 7

// Días que se conserva una partida en la que nadie ha movido ninguna pieza.
// Las partidas ya empezadas y sin terminar no se borran nunca.
const diasConservarSinMovimientos = 14

// borrarPartidas borra de la base de datos y de la memoria las partidas
// que cumplan la condición SQL indicada y devuelve cuántas ha borrado.
func borrarPartidas(condicion string, dias int) int {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, `DELETE FROM partidas
WHERE `+condicion+`
AND actualizada < now() - make_interval(days => $1::int)
RETURNING codigo`, dias)
	if err != nil {
		log.Printf("Error en la limpieza de partidas: %v", err)
		return 0
	}
	defer rows.Close()

	borradas := 0

	for rows.Next() {
		var codigo string

		if rows.Scan(&codigo) != nil {
			continue
		}

		games.Lock()
		delete(games.m, codigo)
		games.Unlock()

		borradas++
	}

	return borradas
}

// limpiarPartidas borra las partidas terminadas antiguas y las que
// se crearon y nunca se jugaron.
func limpiarPartidas() {
	if db == nil {
		return
	}

	terminadas := borrarPartidas("terminada = TRUE", diasConservarTerminadas)
	sinJugar := borrarPartidas("terminada = FALSE AND movimientos = ''", diasConservarSinMovimientos)

	log.Printf(
		"Limpieza: %d partidas terminadas y %d partidas sin jugar borradas",
		terminadas,
		sinJugar,
	)
}

// hayJaque indica si el último movimiento de la lista da jaque.
func hayJaque(movimientos []string) bool {
	if len(movimientos) == 0 {
		return false
	}

	partida := chess.NewGame()

	for _, uci := range movimientos[:len(movimientos)-1] {
		if !jugarUCI(partida, uci) {
			return false
		}
	}

	ultimo := movimientos[len(movimientos)-1]

	for _, candidato := range partida.ValidMoves() {
		if candidato.String() == ultimo {
			notacion := chess.AlgebraicNotation{}.Encode(
				partida.Position(),
				candidato,
			)

			return strings.HasSuffix(notacion, "+")
		}
	}

	return false
}

// clienteFCM es nil si no hay FIREBASE_CREDENTIALS (no se envían notificaciones).
var clienteFCM *messaging.Client

func iniciarFirebase(credenciales string) error {
	ctx := context.Background()

	app, err := firebase.NewApp(ctx, nil, option.WithCredentialsJSON([]byte(credenciales)))
	if err != nil {
		return err
	}

	clienteFCM, err = app.Messaging(ctx)

	return err
}

// borrarToken elimina un token que Firebase ha dado por inválido.
func borrarToken(codigo string, color chess.Color, token string) {
	games.RLock()
	g := games.m[codigo]
	games.RUnlock()

	if g == nil {
		return
	}

	g.Mu.Lock()
	defer g.Mu.Unlock()

	if color == chess.White && g.TokenBlancas == token {
		g.TokenBlancas = ""
		guardarPartida(g)
	}

	if color == chess.Black && g.TokenNegras == token {
		g.TokenNegras = ""
		guardarPartida(g)
	}
}

// enviarAviso manda una notificación al móvil del jugador indicado.
// Se envía en segundo plano para no retrasar la partida.
// Si la app está abierta en ese móvil, Android no muestra el aviso.
func enviarAviso(g *Game, color chess.Color, texto string) {
	if clienteFCM == nil {
		return
	}

	token := g.TokenBlancas
	if color == chess.Black {
		token = g.TokenNegras
	}

	if token == "" {
		return
	}

	go func(codigo string, color chess.Color, token string) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		_, err := clienteFCM.Send(ctx, &messaging.Message{
			Token: token,
			Notification: &messaging.Notification{
				Title: "ChachiChess · " + codigo,
				Body:  texto,
			},
			Data: map[string]string{
				"codigo": codigo,
			},
			Android: &messaging.AndroidConfig{
				Priority: "high",
				Notification: &messaging.AndroidNotification{
					ChannelID:   "turnos_v2",
					Sound:       "aviso_turno",
					Tag:         codigo,
					ClickAction: "com.example.chachichess.ABRIR_PARTIDA",
				},
			},
		})
		if err != nil {
			log.Printf("Error enviando notificación (%s): %v", codigo, err)

			if messaging.IsUnregistered(err) {
				borrarToken(codigo, color, token)
			}
		}
	}(g.Code, color, token)
}

// jugarUCI aplica un movimiento UCI a una partida si es legal.
func jugarUCI(partida *chess.Game, uci string) bool {
	for _, candidato := range partida.ValidMoves() {
		if candidato.String() == uci {
			return partida.Move(candidato) == nil
		}
	}

	return false
}

// cargarPartidas recupera todas las partidas guardadas al arrancar,
// rehaciendo cada una a partir de su historial de movimientos.
func cargarPartidas() error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, `SELECT codigo, blancas, negras, movimientos, terminada, resultado, abandona, token_blancas, token_negras FROM partidas`)
	if err != nil {
		return err
	}
	defer rows.Close()

	total := 0

	for rows.Next() {
		var codigo, blancas, negras, movimientos, resultado, tokenBlancas, tokenNegras string
		var terminada bool
		var abandona int

		err = rows.Scan(&codigo, &blancas, &negras, &movimientos, &terminada, &resultado, &abandona, &tokenBlancas, &tokenNegras)
		if err != nil {
			return err
		}

		partida := chess.NewGame()
		aplicados := []string{}

		if movimientos != "" {
			for _, uci := range strings.Split(movimientos, ",") {
				if !jugarUCI(partida, uci) {
					log.Printf("Partida %s: movimiento %s no se pudo rehacer", codigo, uci)
					break
				}

				aplicados = append(aplicados, uci)
			}
		}

		games.Lock()
		games.m[codigo] = &Game{
			Code:        codigo,
			Game:        partida,
			Clients:     make(map[*websocket.Conn]bool),
			Blancas:     blancas,
			Negras:      negras,
			Movimientos: aplicados,
			Terminada:   terminada,
			Resultado:   resultado,
			Abandona:    chess.Color(abandona),
			EnJaque:     !terminada && hayJaque(aplicados),

			TokenBlancas: tokenBlancas,
			TokenNegras:  tokenNegras,
		}
		games.Unlock()

		total++
	}

	log.Printf("Partidas recuperadas de la base de datos: %d", total)

	return rows.Err()
}

func main() {
	cadena := os.Getenv("DATABASE_URL")

	if cadena == "" {
		log.Println("AVISO: no hay DATABASE_URL; las partidas solo se guardan en memoria")
	} else {
		if err := iniciarBD(cadena); err != nil {
			log.Fatalf("No se pudo conectar con la base de datos: %v", err)
		}

		if err := cargarPartidas(); err != nil {
			log.Fatalf("No se pudieron cargar las partidas: %v", err)
		}

		// Limpieza al arrancar y después cada 6 horas.
		go func() {
			limpiarPartidas()

			for range time.Tick(6 * time.Hour) {
				limpiarPartidas()
			}
		}()
	}

	credenciales := os.Getenv("FIREBASE_CREDENTIALS")

	if credenciales == "" {
		log.Println("AVISO: no hay FIREBASE_CREDENTIALS; no se enviarán notificaciones")
	} else if err := iniciarFirebase(credenciales); err != nil {
		log.Printf("Firebase no disponible, no se enviarán notificaciones: %v", err)
	} else {
		log.Println("Notificaciones Firebase activadas")
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Servidor de ajedrez activo"))
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("OK"))
	})

	http.HandleFunc("/estado", estadoHandler)
	http.HandleFunc("/game", gameHandler)
	http.HandleFunc("/game/", gameHandler)

	puerto := os.Getenv("PORT")

	if puerto == "" {
		puerto = "8080"
	}

	log.Printf("Servidor escuchando en :%s", puerto)
	log.Fatal(http.ListenAndServe(":"+puerto, nil))
}

func movimientoUCI(mensaje string) (string, error) {
	partes := strings.Split(mensaje, "-")

	if len(partes) != 2 && len(partes) != 3 {
		return "", fmt.Errorf("movimiento incorrecto")
	}

	origen := strings.Split(partes[0], ",")
	destino := strings.Split(partes[1], ",")

	if len(origen) != 2 || len(destino) != 2 {
		return "", fmt.Errorf("coordenadas incorrectas")
	}

	filaOrigen, err1 := strconv.Atoi(origen[0])
	columnaOrigen, err2 := strconv.Atoi(origen[1])
	filaDestino, err3 := strconv.Atoi(destino[0])
	columnaDestino, err4 := strconv.Atoi(destino[1])

	if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
		return "", fmt.Errorf("coordenadas no numéricas")
	}

	if filaOrigen < 0 || filaOrigen > 7 ||
		filaDestino < 0 || filaDestino > 7 ||
		columnaOrigen < 0 || columnaOrigen > 7 ||
		columnaDestino < 0 || columnaDestino > 7 {
		return "", fmt.Errorf("casilla incorrecta")
	}

	origenUCI := string(rune('a'+columnaOrigen)) +
		strconv.Itoa(8-filaOrigen)

	destinoUCI := string(rune('a'+columnaDestino)) +
		strconv.Itoa(8-filaDestino)

	resultado := origenUCI + destinoUCI

	// Tercer campo opcional: pieza de coronación (q, r, b, n).
	if len(partes) == 3 {
		switch partes[2] {
		case "q", "r", "b", "n":
			resultado += partes[2]
		default:
			return "", fmt.Errorf("pieza de coronación incorrecta")
		}
	}

	return resultado, nil
}

// nombreColor devuelve "white" o "black", que es lo que entiende la app.
func nombreColor(c chess.Color) string {
	if c == chess.White {
		return "white"
	}
	return "black"
}

// snapshotPartida construye el mensaje de estado completo:
// ESTADO|COLOR|FEN|MOVIMIENTOS
// La FEN contiene la posición y de quién es el turno.
// Debe llamarse con g.Mu bloqueado.
func snapshotPartida(g *Game, color chess.Color) string {
	return "ESTADO|" + nombreColor(color) + "|" + g.Game.FEN() + "|" +
		strings.Join(g.Movimientos, ",")
}

// textoResultado devuelve el texto del resultado para un jugador concreto.
// Debe llamarse con g.Mu bloqueado.
func textoResultado(g *Game, color chess.Color) string {
	if g.Resultado == "Abandono" {
		if g.Abandona == color {
			return "Has abandonado"
		}
		return "Victoria por abandono"
	}
	return g.Resultado
}

type consultaEstado struct {
	Codigo  string `json:"codigo"`
	Jugador string `json:"jugador"`
}

type respuestaEstado struct {
	Codigo         string `json:"codigo"`
	Existe         bool   `json:"existe"`
	TuTurno        bool   `json:"tuTurno"`
	Terminada      bool   `json:"terminada"`
	EsperandoRival bool   `json:"esperandoRival"`
}

// estadoHandler recibe una lista de partidas con el identificador del jugador
// y devuelve, para cada una, si existe y a quién le toca mover.
// Solo se dan datos de una partida si el identificador pertenece a uno de sus jugadores.
func estadoHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var consultas []consultaEstado

	err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&consultas)
	if err != nil {
		http.Error(w, "petición incorrecta", http.StatusBadRequest)
		return
	}

	if len(consultas) > 100 {
		consultas = consultas[:100]
	}

	respuestas := make([]respuestaEstado, 0, len(consultas))

	for _, consulta := range consultas {
		codigo := strings.ToUpper(strings.TrimSpace(consulta.Codigo))
		respuesta := respuestaEstado{Codigo: consulta.Codigo}

		games.RLock()
		g := games.m[codigo]
		games.RUnlock()

		if g != nil && consulta.Jugador != "" {
			g.Mu.Lock()

			esJugador := false
			var color chess.Color

			switch consulta.Jugador {
			case g.Blancas:
				esJugador = true
				color = chess.White
			case g.Negras:
				esJugador = true
				color = chess.Black
			}

			if esJugador {
				respuesta.Existe = true
				respuesta.Terminada = g.Terminada
				respuesta.EsperandoRival = g.Blancas == "" || g.Negras == ""
				respuesta.TuTurno = g.Game.Position().Turn() == color
			}

			g.Mu.Unlock()
		}

		respuestas = append(respuestas, respuesta)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(respuestas)
}

func gameHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		// Buscar un código que no esté en uso (hay 24.000 combinaciones posibles).
		games.Lock()

		c := ""

		for intento := 0; intento < 1000; intento++ {
			candidato := code()

			if games.m[candidato] == nil {
				c = candidato
				break
			}
		}

		if c == "" {
			games.Unlock()
			http.Error(
				w,
				"no hay códigos de partida libres",
				http.StatusServiceUnavailable,
			)
			return
		}

		nueva := &Game{
			Code:    c,
			Game:    chess.NewGame(),
			Clients: make(map[*websocket.Conn]bool),
		}

		games.m[c] = nueva
		games.Unlock()

		nueva.Mu.Lock()
		guardarPartida(nueva)
		nueva.Mu.Unlock()

		_ = json.NewEncoder(w).Encode(map[string]string{
			"code": c,
		})
		return
	}

	if r.Method != http.MethodGet {
		http.Error(
			w,
			"method not allowed",
			http.StatusMethodNotAllowed,
		)
		return
	}

	c := strings.ToUpper(
		strings.TrimPrefix(r.URL.Path, "/game/"),
	)

	games.RLock()
	g := games.m[c]
	games.RUnlock()

	if g == nil {
		http.Error(
			w,
			"partida no encontrada",
			http.StatusNotFound,
		)
		return
	}

	jugadorID := strings.TrimSpace(r.URL.Query().Get("jugador"))
	if jugadorID == "" {
		http.Error(
			w,
			"falta el identificador del jugador",
			http.StatusBadRequest,
		)
		return
	}

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	// Identificar al jugador: si ya es uno de los dos, recupera su color;
	// si no, ocupa el primer hueco libre; si no hay hueco, se rechaza.
	g.Mu.Lock()

	var miColor chess.Color
	nuevoJugador := false

	switch {
	case jugadorID == g.Blancas:
		miColor = chess.White
	case jugadorID == g.Negras:
		miColor = chess.Black
	case g.Blancas == "":
		g.Blancas = jugadorID
		miColor = chess.White
		nuevoJugador = true
	case g.Negras == "":
		g.Negras = jugadorID
		miColor = chess.Black
		nuevoJugador = true
	default:
		g.Mu.Unlock()
		_ = ws.WriteMessage(
			websocket.TextMessage,
			[]byte("RECHAZADO:La partida ya tiene dos jugadores"),
		)
		_ = ws.Close()
		return
	}

	g.Clients[ws] = true

	// Estado completo de la partida para este jugador.
	_ = ws.WriteMessage(
		websocket.TextMessage,
		[]byte(snapshotPartida(g, miColor)),
	)

	if g.Terminada {
		_ = ws.WriteMessage(
			websocket.TextMessage,
			[]byte("RESULTADO:"+textoResultado(g, miColor)),
		)
	}

	if g.EnJaque && !g.Terminada {
		_ = ws.WriteMessage(
			websocket.TextMessage,
			[]byte("ESTADO:Jaque"),
		)
	}

	if nuevoJugador {
		guardarPartida(g)
	}

	g.Mu.Unlock()

	log.Printf(
		"Jugador conectado a %s como %s",
		g.Code,
		nombreColor(miColor),
	)

	defer func() {
		g.Mu.Lock()
		delete(g.Clients, ws)
		g.Mu.Unlock()
		_ = ws.Close()
	}()

	for {
		_, mensaje, err := ws.ReadMessage()
		if err != nil {
			return
		}

		// El móvil envía su identificador de notificaciones (no se escribe en el log).
		if strings.HasPrefix(string(mensaje), "TOKEN|") {
			token := strings.TrimSpace(strings.TrimPrefix(string(mensaje), "TOKEN|"))

			g.Mu.Lock()

			if token != "" {
				if miColor == chess.White && g.TokenBlancas != token {
					g.TokenBlancas = token
					guardarPartida(g)
				} else if miColor == chess.Black && g.TokenNegras != token {
					g.TokenNegras = token
					guardarPartida(g)
				}
			}

			g.Mu.Unlock()
			continue
		}

		log.Printf(
			"Mensaje recibido: %s",
			string(mensaje),
		)

		if string(mensaje) == "ABANDONAR" {
			g.Mu.Lock()

			if !g.Terminada {
				g.Terminada = true
				g.Resultado = "Abandono"
				g.Abandona = miColor

				for cliente := range g.Clients {
					if cliente == ws {
						_ = cliente.WriteMessage(
							websocket.TextMessage,
							[]byte("RESULTADO:Has abandonado"),
						)
					} else {
						_ = cliente.WriteMessage(
							websocket.TextMessage,
							[]byte("RESULTADO:Victoria por abandono"),
						)
					}
				}

				rival := chess.White
				if miColor == chess.White {
					rival = chess.Black
				}

				enviarAviso(g, rival, "Tu rival ha abandonado: has ganado")

				guardarPartida(g)
			}

			g.Mu.Unlock()
			continue
		}

		uci, err := movimientoUCI(string(mensaje))
		if err != nil {
			log.Printf(
				"Movimiento incorrecto: %v",
				err,
			)

			g.Mu.Lock()
			_ = ws.WriteMessage(
				websocket.TextMessage,
				[]byte("ERROR:Movimiento no permitido"),
			)
			g.Mu.Unlock()
			continue
		}

		g.Mu.Lock()

		if g.Terminada {
			_ = ws.WriteMessage(
				websocket.TextMessage,
				[]byte("ERROR:La partida ha terminado"),
			)

			g.Mu.Unlock()
			continue
		}

		if g.Game.Position().Turn() != miColor {
			_ = ws.WriteMessage(
				websocket.TextMessage,
				[]byte("ERROR:No es tu turno"),
			)

			g.Mu.Unlock()
			continue
		}

		var movimiento *chess.Move

		for _, candidato := range g.Game.ValidMoves() {
			if candidato.String() == uci {
				movimiento = candidato
				break
			}
		}

		if movimiento == nil {
			log.Printf(
				"Movimiento rechazado (%s): movimiento no válido",
				uci,
			)

			textoError := "ERROR:Movimiento no permitido"
			if g.EnJaque {
				textoError = "ERROR:Estás en jaque: protege a tu rey"
			}

			_ = ws.WriteMessage(
				websocket.TextMessage,
				[]byte(textoError),
			)

			g.Mu.Unlock()
			continue
		}

		posicionAnterior := g.Game.Position()

		err = g.Game.Move(movimiento)

		if err != nil {
			log.Printf(
				"Movimiento rechazado (%s): %v",
				uci,
				err,
			)

			_ = ws.WriteMessage(
				websocket.TextMessage,
				[]byte("ERROR:Movimiento no permitido"),
			)

			g.Mu.Unlock()
			continue
		}

		g.Movimientos = append(g.Movimientos, uci)

		notacion := chess.AlgebraicNotation{}.Encode(
			posicionAnterior,
			movimiento,
		)

		log.Printf(
			"Movimiento aceptado: %s",
			uci,
		)

		log.Printf(
			"Notación: %s",
			notacion,
		)

		var mensajeEstado string

		switch {
		case g.Game.Method() == chess.Checkmate:
			mensajeEstado = "RESULTADO:Jaque mate"
			g.Terminada = true
			g.Resultado = "Jaque mate"

		case g.Game.Method() == chess.Stalemate:
			mensajeEstado = "RESULTADO:Tablas"
			g.Terminada = true
			g.Resultado = "Tablas"

		case strings.HasSuffix(notacion, "+"):
			mensajeEstado = "ESTADO:Jaque"
		}

		g.EnJaque = !g.Terminada && strings.HasSuffix(notacion, "+")

		for cliente := range g.Clients {
			_ = cliente.WriteMessage(
				websocket.TextMessage,
				mensaje,
			)
		}

		// Posición completa tras el movimiento (corrige enroque, captura al paso y coronación).
		for cliente := range g.Clients {
			_ = cliente.WriteMessage(
				websocket.TextMessage,
				[]byte("TABLERO|"+g.Game.FEN()),
			)
		}

		// El aviso de jaque o el resultado van después del movimiento.
		if mensajeEstado != "" {
			for cliente := range g.Clients {
				_ = cliente.WriteMessage(
					websocket.TextMessage,
					[]byte(mensajeEstado),
				)
			}
		}

		// Notificación al móvil del jugador al que le toca (o al afectado por el resultado).
		siguiente := g.Game.Position().Turn()

		switch {
		case g.Resultado == "Jaque mate":
			enviarAviso(g, siguiente, "Jaque mate: has perdido la partida")
		case g.Resultado == "Tablas":
			enviarAviso(g, chess.White, "Tablas")
			enviarAviso(g, chess.Black, "Tablas")
		case !g.Terminada && g.EnJaque:
			enviarAviso(g, siguiente, "¡Estás en jaque! Es tu turno")
		case !g.Terminada:
			enviarAviso(g, siguiente, "Es tu turno")
		}

		// Se guarda después de enviar el movimiento, para no retrasar al rival.
		guardarPartida(g)

		g.Mu.Unlock()
	}
}
