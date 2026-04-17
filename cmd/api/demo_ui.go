package main

import (
	"embed"
	"io/fs"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

var (
	//go:embed ui/*
	demoUIEmbedFS embed.FS
	demoUIFS      fs.FS
)

func init() {
	sub, err := fs.Sub(demoUIEmbedFS, "ui")
	if err != nil {
		log.Printf("[API] demo ui disabled: %v", err)
		return
	}
	demoUIFS = sub
}

func registerDemoUIRoutes(router gin.IRoutes) {
	if router == nil || demoUIFS == nil {
		return
	}

	fileServer := http.FileServer(http.FS(demoUIFS))

	router.GET("/demo", func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "/demo/")
	})
	router.GET("/demo/*filepath", gin.WrapH(http.StripPrefix("/demo/", fileServer)))
}
