package telemetry

import (
	"context"
	"os"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.38.0"

	"github.com/argoproj/argo-workflows/v3"
	"github.com/argoproj/argo-workflows/v3/util/logging"
)

func workflowsResource(ctx context.Context, serviceName string) *resource.Resource {
	argoversion := argo.GetVersion()
	attribs := []attribute.KeyValue{
		semconv.ServiceName(serviceName),
		semconv.ServiceVersion(argoversion.Version),
	}
	attribs = append(attribs, envVars()...)

	res, err := resource.New(
		ctx,
		resource.WithFromEnv(),      // Discover and provide attributes from OTEL_RESOURCE_ATTRIBUTES and OTEL_SERVICE_NAME environment variables.
		resource.WithTelemetrySDK(), // Discover and provide information about the OpenTelemetry SDK used.
		resource.WithProcess(),      // Discover and provide process information.
		resource.WithOS(),           // Discover and provide OS information.
		resource.WithContainer(),    // Discover and provide container information.
		resource.WithHost(),         // Discover and provide host information.
		resource.WithAttributes(attribs...),
	)
	if err != nil {
		logging.RequireLoggerFromContext(ctx).WithError(err).Error(ctx, "Error from opentelemetry resource detection, carrying on anyway")
	}
	return res
}

func envVars() []attribute.KeyValue {
	attribs := make([]attribute.KeyValue, 0)
	for _, e := range os.Environ() {
		parts := strings.SplitN(e, "=", 2)
		if len(parts) == 2 {
			attribs = append(attribs, semconv.ProcessEnvironmentVariable(parts[0], parts[1]))
		}
	}
	return attribs
}
