package main

import (
	"testing"
)

func TestElasticsearchDocumentEqual(t *testing.T) {
	cases := []struct {
		name     string
		doc1     ElasticsearchDocument
		doc2     ElasticsearchDocument
		expected bool
	}{
		{"empty", ElasticsearchDocument{}, ElasticsearchDocument{}, true},
		{"simple", ElasticsearchDocument{Id: "12345", Path: "/foo", Label: "foo", Creator: "foo#bar", FileType: "generic", DateCreated: 12345, DateModified: 12346, FileSize: 4444}, ElasticsearchDocument{Id: "12345", Path: "/foo", Label: "foo", Creator: "foo#bar", FileType: "generic", DateCreated: 12345, DateModified: 12346, FileSize: 4444}, true},
		{"simple-negative", ElasticsearchDocument{Id: "1245", Path: "/fo", Label: "fo", Creator: "fo#bar", FileType: "gneric", DateCreated: 1345, DateModified: 1346, FileSize: 444}, ElasticsearchDocument{Id: "12345", Path: "/foo", Label: "foo", Creator: "foo#bar", FileType: "generic", DateCreated: 12345, DateModified: 12346, FileSize: 4444}, false},
		{"metadata", ElasticsearchDocument{Id: "12345", Metadata: []Metadatum{Metadatum{"foo", "bar", "baz"}, Metadatum{"quux", "fool", "bacon"}}}, ElasticsearchDocument{Id: "12345", Metadata: []Metadatum{Metadatum{"foo", "bar", "baz"}, Metadatum{"quux", "fool", "bacon"}}}, true},
		{"metadata-different-length", ElasticsearchDocument{Id: "12345", Metadata: []Metadatum{Metadatum{"quux", "fool", "bacon"}}}, ElasticsearchDocument{Id: "12345", Metadata: []Metadatum{Metadatum{"foo", "bar", "baz"}, Metadatum{"quux", "fool", "bacon"}}}, false},
		{"metadata-different-length-2", ElasticsearchDocument{Id: "12345", Metadata: []Metadatum{Metadatum{"foo", "bar", "baz"}, Metadatum{"quux", "fool", "bacon"}}}, ElasticsearchDocument{Id: "12345", Metadata: []Metadatum{Metadatum{"foo", "bar", "baz"}}}, false},
		{"metadata-out-of-order", ElasticsearchDocument{Id: "12345", Metadata: []Metadatum{Metadatum{"quux", "fool", "bacon"}, Metadatum{"foo", "bar", "baz"}}}, ElasticsearchDocument{Id: "12345", Metadata: []Metadatum{Metadatum{"foo", "bar", "baz"}, Metadatum{"quux", "fool", "bacon"}}}, true},
		{"perms", ElasticsearchDocument{Path: "/foo/bar", UserPermissions: []UserPermission{UserPermission{"foo#bar", "read"}, UserPermission{"quux#bar", "write"}}}, ElasticsearchDocument{Path: "/foo/bar", UserPermissions: []UserPermission{UserPermission{"foo#bar", "read"}, UserPermission{"quux#bar", "write"}}}, true},
		{"perms-different-length", ElasticsearchDocument{Path: "/foo/bar", UserPermissions: []UserPermission{UserPermission{"quux#bar", "write"}}}, ElasticsearchDocument{Path: "/foo/bar", UserPermissions: []UserPermission{UserPermission{"foo#bar", "read"}, UserPermission{"quux#bar", "write"}}}, false},
		{"perms-different-length-2", ElasticsearchDocument{Path: "/foo/bar", UserPermissions: []UserPermission{UserPermission{"foo#bar", "read"}, UserPermission{"quux#bar", "write"}}}, ElasticsearchDocument{Path: "/foo/bar", UserPermissions: []UserPermission{UserPermission{"foo#bar", "read"}}}, false},
		{"perms-out-of-order", ElasticsearchDocument{Path: "/foo/bar", UserPermissions: []UserPermission{UserPermission{"foo#bar", "read"}, UserPermission{"quux#bar", "write"}}}, ElasticsearchDocument{Path: "/foo/bar", UserPermissions: []UserPermission{UserPermission{"quux#bar", "write"}, UserPermission{"foo#bar", "read"}}}, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res1 := c.doc1.Equal(c.doc2)
			res2 := c.doc2.Equal(c.doc1)
			if res1 != res2 {
				t.Error("Got different results for comparisons in opposite directions")
			}

			if res1 != c.expected {
				t.Errorf("Got %t instead of expected %t", res1, c.expected)
			}
		})
	}
}
