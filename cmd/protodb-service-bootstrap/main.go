package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"unicode"

	"github.com/spf13/pflag"
)

const protoTemplate = `syntax = "proto3";

package {{ .Package }};

import "{{ .ProtoImport }}";
import "filters/field_filter.proto";
import "google/protobuf/field_mask.proto";
import "patch/go.proto";

option (go.lint) = {all: true};

option go_package = "{{ GoPackageOption .GoPackage }}";

service {{ .Resource }}Service {
  rpc Create(CreateRequest) returns (CreateResponse);
  rpc Read(ReadRequest) returns (ReadResponse);
  rpc Update(UpdateRequest) returns (UpdateResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
  rpc List(ListRequest) returns (ListResponse);
  rpc Watch(WatchRequest) returns (stream WatchResponse);
}

message CreateRequest {
  {{ .Resource }} {{ SnakeCase .Resource }} = 1;
}

message CreateResponse {
  {{ .Resource }} {{ SnakeCase .Resource }} = 1;
}

message ReadRequest {
  string id = 1;
  google.protobuf.FieldMask fields = 2;
}

message ReadResponse {
  {{ .Resource }} {{ SnakeCase .Resource }} = 1;
}

message UpdateRequest {
  {{ .Resource }} {{ SnakeCase .Resource }} = 1;
  google.protobuf.FieldMask fields = 2;
}

message UpdateResponse {
  {{ .Resource }} {{ SnakeCase .Resource }} = 1;
}

message DeleteRequest {
  string id = 1;
}

message DeleteResponse {}

message ListRequest {
  linka.cloud.protofilters.Expression filter = 1;
  uint64 limit = 2;
  uint64 offset = 3;
  string token = 4;
  google.protobuf.FieldMask fields = 5;
}

message ListResponse {
  repeated {{ .Resource }} {{ SnakeCase .Resource | Plural }} = 1;
  bool has_next = 2;
  string token = 3;
}

message WatchRequest {
  linka.cloud.protofilters.Expression filter = 1;
  google.protobuf.FieldMask fields = 2;
}

message WatchResponse {
  enum Type {
    UNKNOWN = 0;
    ENTER = 1;
    LEAVE = 2;
    UPDATE = 3;
  }
  Type type = 1;
  {{ .Resource }} old = 2;
  {{ .Resource }} new = 3;
}
`

type templateData struct {
	Resource    string
	Package     string
	GoPackage   string
	ProtoImport string
}

type protoMeta struct {
	Package   string
	GoPackage string
	Messages  map[string]struct{}
}

func main() {
	resource := pflag.String("resource", "", "resource message name (e.g. Resource or pkg.Resource)")
	protoPath := pflag.String("proto", "", "source proto file defining the resource")
	out := pflag.String("out", "", "output .proto file path (optional)")
	pflag.Parse()

	if *resource == "" || *protoPath == "" {
		fmt.Fprintln(os.Stderr, "--resource and --proto are required")
		os.Exit(2)
	}

	meta, err := parseProtoMeta(*protoPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	resolvedResource, err := resolveResource(*resource, meta.Package, meta.Messages)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	output := *out
	if output == "" {
		output = outputPath(*protoPath, resolvedResource)
	}

	data := templateData{
		Resource:    resolvedResource,
		Package:     meta.Package,
		GoPackage:   meta.GoPackage,
		ProtoImport: protoImportPath(*protoPath),
	}

	proto, err := renderProto(data)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if err := os.WriteFile(output, proto, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseProtoMeta(path string) (protoMeta, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return protoMeta{}, err
	}

	meta := protoMeta{Messages: map[string]struct{}{}}
	inBlock := false
	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		line = stripComments(line, &inBlock)
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "package ") {
			name := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(line, "package "), ";"))
			if meta.Package == "" {
				meta.Package = name
			}
			continue
		}
		if strings.Contains(line, "go_package") && strings.Contains(line, "option") {
			if value, ok := parseQuotedValue(line); ok {
				meta.GoPackage = value
			}
			continue
		}
		if strings.HasPrefix(line, "message ") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				name := strings.TrimSuffix(fields[1], "{")
				meta.Messages[name] = struct{}{}
			}
		}
	}

	if meta.Package == "" {
		return protoMeta{}, fmt.Errorf("package not found in %s", path)
	}
	if meta.GoPackage == "" {
		return protoMeta{}, fmt.Errorf("go_package option not found in %s", path)
	}
	return meta, nil
}

func parseQuotedValue(line string) (string, bool) {
	start := strings.Index(line, "\"")
	if start == -1 {
		return "", false
	}
	line = line[start+1:]
	end := strings.Index(line, "\"")
	if end == -1 {
		return "", false
	}
	return line[:end], true
}

func stripComments(line string, inBlock *bool) string {
	value := line
	for {
		if *inBlock {
			end := strings.Index(value, "*/")
			if end == -1 {
				return ""
			}
			value = value[end+2:]
			*inBlock = false
			continue
		}
		blockStart := strings.Index(value, "/*")
		lineStart := strings.Index(value, "//")
		if lineStart >= 0 && (blockStart == -1 || lineStart < blockStart) {
			value = value[:lineStart]
		}
		if blockStart >= 0 {
			end := strings.Index(value[blockStart+2:], "*/")
			if end >= 0 {
				value = value[:blockStart] + value[blockStart+2+end+2:]
				continue
			}
			value = value[:blockStart]
			*inBlock = true
		}
		break
	}
	return value
}

func resolveResource(resource, pkg string, messages map[string]struct{}) (string, error) {
	name := resource
	if strings.Contains(resource, ".") {
		parts := strings.Split(resource, ".")
		if len(parts) != 2 {
			return "", fmt.Errorf("invalid resource %s", resource)
		}
		if parts[0] != pkg {
			return "", fmt.Errorf("resource package %s does not match %s", parts[0], pkg)
		}
		name = parts[1]
	}
	if _, ok := messages[name]; !ok {
		return "", fmt.Errorf("resource %s not found in proto", name)
	}
	return name, nil
}

func outputPath(protoPath, resource string) string {
	filename := snakeCase(resource) + "_service.proto"
	dir := filepath.Dir(protoPath)
	if dir == "." || dir == "" {
		return filename
	}
	return filepath.Join(dir, filename)
}

func protoImportPath(protoPath string) string {
	return filepath.Base(protoPath)
}

func renderProto(data templateData) ([]byte, error) {
	tmpl, err := template.New("crud").Funcs(template.FuncMap{
		"GoPackageOption": goPackageOption,
		"SnakeCase":       snakeCase,
		"Plural":          plural,
	}).Parse(protoTemplate)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func goPackageOption(value string) string {
	if value == "" {
		return ""
	}
	parts := strings.Split(value, ";")
	if len(parts) > 1 {
		return value
	}
	path := strings.TrimRight(value, "/")
	name := value
	if idx := strings.LastIndex(path, "/"); idx >= 0 {
		name = path[idx+1:]
	}
	return value + ";" + name
}

func snakeCase(value string) string {
	if value == "" {
		return value
	}
	var b strings.Builder
	for i, r := range value {
		if unicode.IsUpper(r) {
			if i > 0 {
				b.WriteByte('_')
			}
			b.WriteRune(unicode.ToLower(r))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func plural(value string) string {
	if value == "" {
		return value
	}
	lower := strings.ToLower(value)
	if strings.HasSuffix(lower, "y") && len(value) > 1 {
		prev := lower[len(lower)-2]
		if prev != 'a' && prev != 'e' && prev != 'i' && prev != 'o' && prev != 'u' {
			return value[:len(value)-1] + "ies"
		}
	}
	if strings.HasSuffix(lower, "s") {
		return value + "es"
	}
	return value + "s"
}
