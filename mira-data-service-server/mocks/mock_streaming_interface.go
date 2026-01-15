/*
*

	@author: shiliang
	@date: 2024/12/19
	@note:

*
*/
package mocks

import (
	"context"
	pb "data-service/generated/datasource"
	"google.golang.org/grpc/metadata"
)

type ConcreteClientStreamingServer interface {
	Recv() (*pb.OSSWriteRequest, error)
	SendAndClose(*pb.Response) error
	SetHeader(metadata.MD) error
	SendHeader(metadata.MD) error
	SetTrailer(metadata.MD)
	Context() context.Context
	Header() (metadata.MD, error)
	SendMsg(m any) error
	RecvMsg(m any) error
	Send(*pb.ArrowResponse) error
}
