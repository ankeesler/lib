package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpc_validator "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type GRPCServerConfig struct {
	Port uint16

	PreServeFunc  func(context.Context, *GRPCServer) error
	PostServeFunc func(context.Context, *GRPCServer) error
}

type GRPCServer struct {
	c *GRPCServerConfig

	port uint16
	s    *grpc.Server

	l net.Listener
}

func NewGRPCServer(c *GRPCServerConfig) *GRPCServer {
	s := GRPCServer{
		c: c,
	}

	if c.Port != 0 {
		s.port = c.Port
	} else if port, ok := os.LookupEnv("PORT"); ok {
		port, err := strconv.ParseUint(port, 10, 16)
		if err != nil {
			log.Printf("warning: cannot use PORT env var: %s", err.Error())
			s.port = 0
		} else {
			s.port = uint16(port)
		}
	}

	s.s = grpc.NewServer(
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			streamLogFunc,
			grpc_ctxtags.StreamServerInterceptor(),
			grpc_validator.StreamServerInterceptor(),
			grpc_recovery.StreamServerInterceptor(),
		)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			unaryLogFunc,
			grpc_ctxtags.UnaryServerInterceptor(),
			grpc_validator.UnaryServerInterceptor(),
			grpc_recovery.UnaryServerInterceptor(),
		)),
	)

	return &s
}

func (s *GRPCServer) Addr() net.Addr { return s.l.Addr() }

func (s *GRPCServer) Run(ctx context.Context) error {
	var err error
	s.l, err = net.Listen("tcp", fmt.Sprintf(":%d", s.port)) // Closed by grpc.Serve()
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	g, ctx := errgroup.WithContext(ctx)

	if s.c.PreServeFunc != nil {
		s.c.PreServeFunc(ctx, s)
	}

	g.Go(func() error {
		go func() {
			<-ctx.Done()
			s.s.GracefulStop()
		}()
		return s.s.Serve(s.l)
	})

	if s.c.PostServeFunc != nil {
		s.c.PostServeFunc(ctx, s)
	}

	return g.Wait()
}

func unaryLogFunc(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	log.Printf("unary req: %s: %v", info.FullMethod, textproto(req))
	rsp, err := handler(ctx, req)
	log.Printf("unary rsp: %s: %v %v", info.FullMethod, textproto(req), err)
	return rsp, err
}

func streamLogFunc(
	srv interface{}, ss grpc.ServerStream,
	info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	log.Printf("stream req: %s", info.FullMethod)
	err := handler(srv, &loggingStream{info: info, ServerStream: ss})
	log.Printf("stream rsp: %s: %v", info.FullMethod, err)
	return err
}

type loggingStream struct {
	info *grpc.StreamServerInfo
	grpc.ServerStream
}

func (l *loggingStream) SendMsg(m interface{}) error {
	log.Printf("stream rsp: %s: %v...", l.info.FullMethod, textproto(m))
	return l.ServerStream.SendMsg(m)
}

func textproto(i any) string {
	return prototext.MarshalOptions{
		Multiline: true,
	}.Format(i.(protoreflect.ProtoMessage))
}
