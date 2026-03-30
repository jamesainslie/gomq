package mgmt

import (
	"net/http"
	"net/url"
	"strings"
)

// route represents a registered endpoint with method and path pattern.
type route struct {
	method  string
	pattern string // segments separated by /, with {param} placeholders
	handler http.HandlerFunc
}

// router is a simple HTTP router that uses raw request URIs to avoid
// premature decoding of %2F (URL-encoded /) in path parameters.
// This is critical for RabbitMQ-compatible APIs where vhost "/" is
// encoded as %2F in the URL path.
type router struct {
	routes []route
}

func newRouter() *router {
	return &router{}
}

func (rt *router) handle(method, pattern string, handler http.HandlerFunc) {
	rt.routes = append(rt.routes, route{
		method:  method,
		pattern: pattern,
		handler: handler,
	})
}

// ServeHTTP matches the request against registered routes using the raw URI.
func (rt *router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rawPath := r.URL.RawPath
	if rawPath == "" {
		rawPath = r.URL.Path
	}

	// Strip query string.
	if idx := strings.IndexByte(rawPath, '?'); idx >= 0 {
		rawPath = rawPath[:idx]
	}

	for _, route := range rt.routes {
		if r.Method != route.method {
			continue
		}

		params, ok := matchRoute(route.pattern, rawPath)
		if !ok {
			continue
		}

		// Store params in the request context via URL query hack:
		// we set them in a way retrievable by pathParam().
		for k, v := range params {
			r = setRouteParam(r, k, v)
		}

		route.handler(w, r)
		return
	}

	http.NotFound(w, r)
}

// matchRoute checks if rawPath matches pattern and extracts named parameters.
// Parameters in the pattern are denoted by {name}. Each parameter captures
// a single raw segment (not decoded) between slashes.
func matchRoute(pattern, rawPath string) (map[string]string, bool) {
	patternParts := strings.Split(strings.Trim(pattern, "/"), "/")
	pathParts := splitRawPath(rawPath)

	if len(patternParts) != len(pathParts) {
		return nil, false
	}

	params := make(map[string]string)
	for i, pp := range patternParts {
		if strings.HasPrefix(pp, "{") && strings.HasSuffix(pp, "}") {
			name := pp[1 : len(pp)-1]
			params[name] = pathParts[i]
		} else if pp != pathParts[i] {
			return nil, false
		}
	}

	return params, true
}

// splitRawPath splits a raw URL path into segments, preserving percent-encoding.
func splitRawPath(rawPath string) []string {
	trimmed := strings.Trim(rawPath, "/")
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "/")
}

// routeParamKey is a context key prefix for route parameters.
// We store them in the URL query values prefixed with a magic string.
const routeParamPrefix = "__gomq_param_"

// setRouteParam stores a route parameter in the request's URL query.
func setRouteParam(r *http.Request, key, rawValue string) *http.Request {
	q := r.URL.Query()
	q.Set(routeParamPrefix+key, rawValue)
	r.URL.RawQuery = q.Encode()
	return r
}

// pathParam retrieves a route parameter, decoding the percent-encoded value.
func pathParam(r *http.Request, key string) string {
	raw := r.URL.Query().Get(routeParamPrefix + key)
	decoded, err := url.PathUnescape(raw)
	if err != nil {
		return raw
	}
	return decoded
}
