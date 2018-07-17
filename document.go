package main

import (
	set "github.com/deckarep/golang-set"
)

type Metadatum struct {
	Attribute string `json:"attribute"`
	Value     string `json:"value"`
	Unit      string `json:"unit"`
}

type UserPermission struct {
	User       string `json:"user"`
	Permission string `json:"permission"`
}

type ElasticsearchDocument struct {
	Id              string           `json:"id"`
	Path            string           `json:"path"`
	Label           string           `json:"label"`
	Creator         string           `json:"creator"`
	FileType        string           `json:"fileType"`
	DateCreated     int64            `json:"dateCreated"`
	DateModified    int64            `json:"dateModified"`
	FileSize        int64            `json:"fileSize"`
	Metadata        []Metadatum      `json:"metadata"`
	UserPermissions []UserPermission `json:"userPermissions"`
}

func (doc ElasticsearchDocument) Equal(other ElasticsearchDocument) bool {
	if doc.Id != other.Id {
		return false
	}
	if doc.Path != other.Path {
		return false
	}
	if doc.Label != other.Label {
		return false
	}
	if doc.Creator != other.Creator {
		return false
	}
	if doc.FileType != other.FileType {
		return false
	}
	if doc.DateCreated != other.DateCreated {
		return false
	}
	if doc.DateModified != other.DateModified {
		return false
	}
	if doc.FileSize != other.FileSize {
		return false
	}

	dm := make([]interface{}, len(doc.Metadata))
	for i := range doc.Metadata {
		dm[i] = doc.Metadata[i]
	}
	om := make([]interface{}, len(other.Metadata))
	for i := range other.Metadata {
		om[i] = other.Metadata[i]
	}
	if !set.NewSetFromSlice(dm).Equal(set.NewSetFromSlice(om)) {
		return false
	}

	dp := make([]interface{}, len(doc.UserPermissions))
	for i := range doc.UserPermissions {
		dp[i] = doc.UserPermissions[i]
	}
	op := make([]interface{}, len(other.UserPermissions))
	for i := range other.UserPermissions {
		op[i] = other.UserPermissions[i]
	}
	if !set.NewSetFromSlice(dp).Equal(set.NewSetFromSlice(op)) {
		return false
	}

	return true
}
