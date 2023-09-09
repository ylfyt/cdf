package handlers

import "cdf/models"

type HandlerCtx struct {
	Fields map[string]*models.FieldInfo
}
