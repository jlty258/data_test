package common

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

type Response struct {
	Code int         `json:"code"`
	Msg  string      `json:"message"`
	Data interface{} `json:"data"`
}

func (r *Response) SetErrCode(errCode int) {
	r.Code = errCode
	r.Msg = ErrCodeMessage[errCode]
}

func (r *Response) SetErrMsg(errMsg string) {
	r.Msg = errMsg
}

func (r *Response) SetData(data interface{}) {
	r.Code = ErrCodeOK
	r.Data = data
}

func (r *Response) SetError(errCode int, errMsg string) {
	r.SetErrCode(errCode)
	r.SetErrMsg(errMsg)
}

func NewResponse(code int, message string, data interface{}) *Response {
	return &Response{Code: code, Msg: message, Data: data}
}

func result(c *gin.Context, response *Response) {
	c.JSON(http.StatusOK, response)
}

func Success(c *gin.Context, data interface{}) {
	result(c, NewResponse(ErrCodeOK, "", data))
}

func Failed(c *gin.Context, code int, message string) {
	result(c, NewResponse(code, message, nil))
}
