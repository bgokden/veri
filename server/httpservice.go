package veriserviceserver

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"

	"github.com/bgokden/veri/state"
	"github.com/gorilla/mux"
)

// var Health = false
// var Ready = false

func GetReady(w http.ResponseWriter, r *http.Request) {
	if state.Ready {
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"alive": true}`)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"alive": false}`)
	}
}

func GetHeath(w http.ResponseWriter, r *http.Request) {
	if state.Health {
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"alive": true}`)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"alive": true}`)
	}
}

func respondWithError(w http.ResponseWriter, code int, message string) {
	respondWithJSON(w, code, map[string]string{"error": message})
}

func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, _ := json.Marshal(payload)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}

// RestApi serves common services needed
func RestApi() {
	log.Println("Rest api stared")
	router := mux.NewRouter()
	router.HandleFunc("/", GetHeath).Methods("GET")
	router.HandleFunc("/health", GetHeath).Methods("GET")
	router.HandleFunc("/ready", GetReady).Methods("GET")
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	log.Printf("Http Server failure: %v\n", http.ListenAndServe(":8000", router))
}
