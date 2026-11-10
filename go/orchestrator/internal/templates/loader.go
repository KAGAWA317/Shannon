package templates

import (
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v3"
)

// LoadTemplateFromFile reads a YAML template from disk.
// LoadTemplateFromFile 从磁盘读取 YAML 模板。
func LoadTemplateFromFile(path string) (*Template, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open template %s: %w", path, err)
	}
	defer f.Close()
	tpl, err := decodeTemplate(f)
	if err != nil {
		return nil, fmt.Errorf("decode template %s: %w", path, err)
	}
	return tpl, nil
}

// LoadTemplate parses a template from the provided reader.
// LoadTemplate 从给定的 reader 解析模板。
func LoadTemplate(r io.Reader) (*Template, error) {
	tpl, err := decodeTemplate(r)
	if err != nil {
		return nil, fmt.Errorf("decode template: %w", err)
	}
	return tpl, nil
}

func decodeTemplate(r io.Reader) (*Template, error) {
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	var tpl Template
	if err := dec.Decode(&tpl); err != nil {
		return nil, err
	}
	return &tpl, nil
}
