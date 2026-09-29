package api

import (
	"github.com/go-chi/chi/v5"
	"github.com/wyw14/solo-039/internal/service"
	"log"
	"net/http"
	"strings"
)

type Server struct {
	runtime *service.Runtime
	plant   *service.Plant
	router  chi.Router
}

func NewServer() *Server {
	plant := service.NewPlant("data")
	report, err := plant.Restore()
	if err != nil {
		log.Fatalf("plant restore failed: %v", err)
	}
	// 损坏尾部已隔离、服务可降级继续；但必须显式提示人工复核。
	if report != nil && report.ManualReview {
		log.Printf("WARNING: journal recovery requires manual review: %s", strings.Join(report.ReviewReasons, "; "))
	}
	s := &Server{runtime: service.NewRuntime("baseline"), plant: plant, router: chi.NewRouter()}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler { return s.router }
