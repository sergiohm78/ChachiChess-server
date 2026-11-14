package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
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

func main() {
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

	if len(partes) != 2 {
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

	return origenUCI + destinoUCI, nil
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

		games.Lock()
		games.m[c] = &Game{
			Code:    c,
			Game:    chess.NewGame(),
			Clients: make(map[*websocket.Conn]bool),
		}
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

	switch {
	case jugadorID == g.Blancas:
		miColor = chess.White
	case jugadorID == g.Negras:
		miColor = chess.Black
	case g.Blancas == "":
		g.Blancas = jugadorID
		miColor = chess.White
	case g.Negras == "":
		g.Negras = jugadorID
		miColor = chess.Black
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

		g.Mu.Unlock()
	}
}
