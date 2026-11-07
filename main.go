package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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

	log.Println("Servidor escuchando en :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
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

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	g.Mu.Lock()
	g.Clients[ws] = true
	g.Mu.Unlock()

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

		uci, err := movimientoUCI(string(mensaje))
		if err != nil {
			log.Printf(
				"Movimiento incorrecto: %v",
				err,
			)

			_ = ws.WriteMessage(
				websocket.TextMessage,
				[]byte("ERROR:Movimiento no permitido"),
			)
			continue
		}

		g.Mu.Lock()

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

		case g.Game.Method() == chess.Stalemate:
			mensajeEstado = "RESULTADO:Tablas"

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
