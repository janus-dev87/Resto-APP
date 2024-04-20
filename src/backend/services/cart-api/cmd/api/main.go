package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/jurabek/cart-api/cmd/config"
	"github.com/jurabek/cart-api/internal/database"
	"github.com/jurabek/cart-api/internal/events"
	grpcsvc "github.com/jurabek/cart-api/internal/grpc"
	"github.com/jurabek/cart-api/internal/handlers"
	"github.com/jurabek/cart-api/internal/instrumentation"
	pbv1 "github.com/jurabek/cart-api/pb/v1"
	"github.com/jurabek/cart-api/pkg/reciever"
	"github.com/redis/go-redis/v9"
	"github.com/swaggo/swag/example/basic/docs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"

	"github.com/jurabek/cart-api/internal/repositories"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	GitCommit string
	Version   string
)

//	@title			Cart API
//	@version		1.0
//	@description	This is a rest api for cart which saves items to redis server
//	@termsOfService	http://swagger.io/terms/

//	@contact.name	API Support
//	@contact.url	http://www.swagger.io/support
//	@contact.email	support@swagger.io

// @license.name	Apache 2.0
// @license.url	http://www.apache.org/licenses/LICENSE-2.0.html
func main() {
	ctx := context.Background()
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	basePath, _ := os.LookupEnv("BASE_PATH")
	docs.SwaggerInfo.BasePath = basePath

	close, err := instrumentation.StartOTEL(ctx)	
	if err != nil {
		log.Fatal().Err(err).Msg("Error starting otel")
	}
	defer close() 

	handleSigterm()
	router := http.NewServeMux()
	cfg := config.Init()

	redisClient, err := initRedis(cfg.RedisHost)
	if err != nil {
		fmt.Print(err)
	}
	cartRepository := repositories.NewCartRepository(redisClient)

	config := sarama.NewConfig()
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second

	

	kafkaConsumer, error := sarama.NewConsumerGroup([]string{cfg.KafkaBroker}, "cart-api", config)
	if error != nil {
		log.Fatal().Err(error).Msg("new consumer failed!")
	}
	msgReciever := reciever.NewMessageReciever(kafkaConsumer, cfg.OrdersTopic)
	go func() {
		recieveErr := msgReciever.Recieve(ctx, events.NewOrderCompletedEventHandler(cartRepository))
		log.Error().Err(recieveErr).Msg("Error recieving messages")
	}()

	go grpcServer(grpcsvc.NewCartGrpcService(cartRepository))

	cartHandler := handlers.NewCartHandler(cartRepository)

	cartBasePath := basePath + "/api/v1/cart"
	router.HandleFunc("POST "+cartBasePath, handlers.ErrorHandler(cartHandler.Create))
	router.HandleFunc("GET "+cartBasePath+"/{id}", handlers.ErrorHandler(cartHandler.Get))
	router.HandleFunc("DELETE "+cartBasePath+"/{id}", handlers.ErrorHandler(cartHandler.Delete))
	router.HandleFunc("PUT "+cartBasePath+"/{id}", handlers.ErrorHandler(cartHandler.Update))
	router.HandleFunc("POST "+cartBasePath+"/{id}/item", handlers.ErrorHandler(cartHandler.AddItem))           // adds item or increments quantity by CartID
	router.HandleFunc("PUT "+cartBasePath+"/{id}/item/{itemID}", handlers.ErrorHandler(cartHandler.UpdateItem)) // updates line item item_id is ignored
	router.HandleFunc("DELETE "+cartBasePath+"/{id}/item/{itemID}", handlers.ErrorHandler(cartHandler.DeleteItem))

	otelRouter := otelhttp.NewHandler(router, "server",
		otelhttp.WithMessageEvents(otelhttp.ReadEvents, otelhttp.WriteEvents),
	)

	log.Info().Msg("Starting server on port 8080...")
	log.Fatal().Err(http.ListenAndServe(":5200", otelRouter))
}

func grpcServer(svc pbv1.CartServiceServer) {
	lis, err := net.Listen("tcp", ":8081")
	if err != nil {
		log.Fatal().Err(err)
	}

	server := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	reflection.Register(server)

	pbv1.RegisterCartServiceServer(server, svc)

	log.Info().Msg("Starting gRPC server on port 8081...")
	if err := server.Serve(lis); err != nil {
		log.Fatal().Err(err)
	}
}

func handleSigterm() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM)
	go func() {
		<-c
		time.Sleep(10 * time.Second)
		os.Exit(0)
	}()
}

func initTracer(ctx context.Context) (*sdktrace.TracerProvider, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			// the service name used to display traces in backends
			semconv.ServiceName("cart-api"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// If the OpenTelemetry Collector is running on a local cluster (minikube or
	// microk8s), it should be accessible through the NodePort service at the
	// `localhost:30080` endpoint. Otherwise, replace `localhost` with the
	// endpoint of your cluster. If you run the app inside k8s, then you can
	// probably connect directly to the service through dns.
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	otelExportEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if otelExportEndpoint == "" {
		otelExportEndpoint = "localhost:4317"
	}
	conn, err := grpc.DialContext(ctx, otelExportEndpoint,
		// Note the use of insecure transport here. TLS is recommended in production.
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection to collector: %w", err)
	}

	// Set up a trace exporter
	traceExporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(traceExporter),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return tp, nil
}

func initMeter(ctx context.Context) (*sdkmetric.MeterProvider, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			// the service name used to display traces in backends
			semconv.ServiceName("cart-api"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	exp, err := stdoutmetric.New()
	if err != nil {
		return nil, err
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp)),
	)
	otel.SetMeterProvider(mp)
	return mp, nil
}

func initRedis(redisHost string) (*redis.Client, error) {
	if redisHost == "" {
		redisHost = ":6379"
	}
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisHost,
	})

	// Enable tracing instrumentation.
	if err := redisotel.InstrumentTracing(redisClient); err != nil {
		log.Fatal().Err(err)
	}

	// Enable metrics instrumentation.
	if err := redisotel.InstrumentMetrics(redisClient); err != nil {
		log.Fatal().Err(err)
	}

	err := database.HealthCheck(context.Background(), redisClient)
	return redisClient, err
}