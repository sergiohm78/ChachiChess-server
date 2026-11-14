package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
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

	"github.com/gorilla/websocket"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/notnil/chess"
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

func code() string {
	b := make([]byte, 3)
	_, _ = rand.Read(b)
	return strings.ToUpper(hex.EncodeToString(b))
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

	return err
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
	(codigo, blancas, negras, movimientos, terminada, resultado, abandona, actualizada)
VALUES ($1, $2, $3, $4, $5, $6, $7, now())
ON CONFLICT (codigo) DO UPDATE SET
	blancas = EXCLUDED.blancas,
	negras = EXCLUDED.negras,
	movimientos = EXCLUDED.movimientos,
	terminada = EXCLUDED.terminada,
	resultado = EXCLUDED.resultado,
	abandona = EXCLUDED.abandona,
	actualizada = now()`,
			g.Code,
			g.Blancas,
			g.Negras,
			strings.Join(g.Movimientos, ","),
			g.Terminada,
			g.Resultado,
			int(g.Abandona),
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

	rows, err := db.QueryContext(ctx, `SELECT codigo, blancas, negras, movimientos, terminada, resultado, abandona FROM partidas`)
	if err != nil {
		return err
	}
	defer rows.Close()

	total := 0

	for rows.Next() {
		var codigo, blancas, negras, movimientos, resultado string
		var terminada bool
		var abandona int

		err = rows.Scan(&codigo, &blancas, &negras, &movimientos, &terminada, &resultado, &abandona)
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

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Servidor de ajedrez activo"))
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("OK"))
	})

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

func gameHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		c := code()

		nueva := &Game{
			Code:    c,
			Game:    chess.NewGame(),
			Clients: make(map[*websocket.Conn]bool),
		}

		guardarPartida(nueva)

		games.Lock()
		games.m[c] = nueva
		games.Unlock()

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

			_ = ws.WriteMessage(
				websocket.TextMessage,
				[]byte("ERROR:Movimiento no permitido"),
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

		if mensajeEstado != "" {
			for cliente := range g.Clients {
				_ = cliente.WriteMessage(
					websocket.TextMessage,
					[]byte(mensajeEstado),
				)
			}
		}

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

		// Se guarda después de enviar el movimiento, para no retrasar al rival.
		guardarPartida(g)

		g.Mu.Unlock()
	}
}
