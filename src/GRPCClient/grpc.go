package GRPCClient

import (
  "google.golang.org/grpc"
  "google.golang.org/grpc/metadata"
  "context"
  "fmt"
)

type wrappedStream struct {
	grpc.ClientStream
}

func (w *wrappedStream) RecvMsg(m any) error {
	//logger("Receive a message (Type: %T) at %v", m, time.Now().Format(time.RFC3339))
	return w.ClientStream.RecvMsg(m)
}

func (w *wrappedStream) SendMsg(m any) error {
	//logger("Send a message (Type: %T) at %v", m, time.Now().Format(time.RFC3339))
	return w.ClientStream.SendMsg(m)
}

func newWrappedStream(s grpc.ClientStream) grpc.ClientStream {
	return &wrappedStream{s}
}

// streamInterceptor is an example stream interceptor.
func GenTokenStreamInterceptor(token string) func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
       nt := fmt.Sprintf("Bearer %s", token)
       ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", nt)
	   s, err := streamer(ctx, desc, cc, method, opts...)
       if err != nil {
         return nil, err
       }
       return newWrappedStream(s), nil
    }
}


func GenTokenUnaryInterceptor(token string) func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
  return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
    nt := fmt.Sprintf("Bearer %s", token)
    ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", nt)
	err := invoker(ctx, method, req, reply, cc, opts...)
	return err
  }
}
