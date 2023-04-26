//
//Copyright 2023 Codenotary Inc. All rights reserved.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v3.11.4
// source: authorization.proto

package protomodel

import (
	_ "github.com/grpc-ecosystem/grpc-gateway/protoc-gen-swagger/options"
	_ "google.golang.org/genproto/googleapis/api/annotations"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type OpenSessionRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Username string `protobuf:"bytes,1,opt,name=username,proto3" json:"username,omitempty"`
	Password string `protobuf:"bytes,2,opt,name=password,proto3" json:"password,omitempty"`
	Database string `protobuf:"bytes,3,opt,name=database,proto3" json:"database,omitempty"`
}

func (x *OpenSessionRequest) Reset() {
	*x = OpenSessionRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_authorization_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *OpenSessionRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*OpenSessionRequest) ProtoMessage() {}

func (x *OpenSessionRequest) ProtoReflect() protoreflect.Message {
	mi := &file_authorization_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use OpenSessionRequest.ProtoReflect.Descriptor instead.
func (*OpenSessionRequest) Descriptor() ([]byte, []int) {
	return file_authorization_proto_rawDescGZIP(), []int{0}
}

func (x *OpenSessionRequest) GetUsername() string {
	if x != nil {
		return x.Username
	}
	return ""
}

func (x *OpenSessionRequest) GetPassword() string {
	if x != nil {
		return x.Password
	}
	return ""
}

func (x *OpenSessionRequest) GetDatabase() string {
	if x != nil {
		return x.Database
	}
	return ""
}

type OpenSessionResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	SessionID           string `protobuf:"bytes,1,opt,name=sessionID,proto3" json:"sessionID,omitempty"`
	ServerUUID          string `protobuf:"bytes,2,opt,name=serverUUID,proto3" json:"serverUUID,omitempty"`
	ExpirationTimestamp int32  `protobuf:"varint,3,opt,name=expirationTimestamp,proto3" json:"expirationTimestamp,omitempty"`
	InactivityTimestamp int32  `protobuf:"varint,4,opt,name=inactivityTimestamp,proto3" json:"inactivityTimestamp,omitempty"`
}

func (x *OpenSessionResponse) Reset() {
	*x = OpenSessionResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_authorization_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *OpenSessionResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*OpenSessionResponse) ProtoMessage() {}

func (x *OpenSessionResponse) ProtoReflect() protoreflect.Message {
	mi := &file_authorization_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use OpenSessionResponse.ProtoReflect.Descriptor instead.
func (*OpenSessionResponse) Descriptor() ([]byte, []int) {
	return file_authorization_proto_rawDescGZIP(), []int{1}
}

func (x *OpenSessionResponse) GetSessionID() string {
	if x != nil {
		return x.SessionID
	}
	return ""
}

func (x *OpenSessionResponse) GetServerUUID() string {
	if x != nil {
		return x.ServerUUID
	}
	return ""
}

func (x *OpenSessionResponse) GetExpirationTimestamp() int32 {
	if x != nil {
		return x.ExpirationTimestamp
	}
	return 0
}

func (x *OpenSessionResponse) GetInactivityTimestamp() int32 {
	if x != nil {
		return x.InactivityTimestamp
	}
	return 0
}

type KeepAliveRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *KeepAliveRequest) Reset() {
	*x = KeepAliveRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_authorization_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *KeepAliveRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*KeepAliveRequest) ProtoMessage() {}

func (x *KeepAliveRequest) ProtoReflect() protoreflect.Message {
	mi := &file_authorization_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use KeepAliveRequest.ProtoReflect.Descriptor instead.
func (*KeepAliveRequest) Descriptor() ([]byte, []int) {
	return file_authorization_proto_rawDescGZIP(), []int{2}
}

type KeepAliveResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *KeepAliveResponse) Reset() {
	*x = KeepAliveResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_authorization_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *KeepAliveResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*KeepAliveResponse) ProtoMessage() {}

func (x *KeepAliveResponse) ProtoReflect() protoreflect.Message {
	mi := &file_authorization_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use KeepAliveResponse.ProtoReflect.Descriptor instead.
func (*KeepAliveResponse) Descriptor() ([]byte, []int) {
	return file_authorization_proto_rawDescGZIP(), []int{3}
}

type CloseSessionRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *CloseSessionRequest) Reset() {
	*x = CloseSessionRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_authorization_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CloseSessionRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CloseSessionRequest) ProtoMessage() {}

func (x *CloseSessionRequest) ProtoReflect() protoreflect.Message {
	mi := &file_authorization_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CloseSessionRequest.ProtoReflect.Descriptor instead.
func (*CloseSessionRequest) Descriptor() ([]byte, []int) {
	return file_authorization_proto_rawDescGZIP(), []int{4}
}

type CloseSessionResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *CloseSessionResponse) Reset() {
	*x = CloseSessionResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_authorization_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CloseSessionResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CloseSessionResponse) ProtoMessage() {}

func (x *CloseSessionResponse) ProtoReflect() protoreflect.Message {
	mi := &file_authorization_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CloseSessionResponse.ProtoReflect.Descriptor instead.
func (*CloseSessionResponse) Descriptor() ([]byte, []int) {
	return file_authorization_proto_rawDescGZIP(), []int{5}
}

var File_authorization_proto protoreflect.FileDescriptor

var file_authorization_proto_rawDesc = []byte{
	0x0a, 0x13, 0x61, 0x75, 0x74, 0x68, 0x6f, 0x72, 0x69, 0x7a, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0c, 0x69, 0x6d, 0x6d, 0x75, 0x64, 0x62, 0x2e, 0x6d, 0x6f,
	0x64, 0x65, 0x6c, 0x1a, 0x1c, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x61, 0x70, 0x69, 0x2f,
	0x61, 0x6e, 0x6e, 0x6f, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x1a, 0x2c, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x2d, 0x67, 0x65, 0x6e, 0x2d, 0x73, 0x77,
	0x61, 0x67, 0x67, 0x65, 0x72, 0x2f, 0x6f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2f, 0x61, 0x6e,
	0x6e, 0x6f, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22,
	0x68, 0x0a, 0x12, 0x4f, 0x70, 0x65, 0x6e, 0x53, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1a, 0x0a, 0x08, 0x75, 0x73, 0x65, 0x72, 0x6e, 0x61, 0x6d,
	0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x75, 0x73, 0x65, 0x72, 0x6e, 0x61, 0x6d,
	0x65, 0x12, 0x1a, 0x0a, 0x08, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x08, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x12, 0x1a, 0x0a,
	0x08, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x08, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x22, 0xb7, 0x01, 0x0a, 0x13, 0x4f, 0x70,
	0x65, 0x6e, 0x53, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x12, 0x1c, 0x0a, 0x09, 0x73, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x49, 0x44, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x73, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x49, 0x44, 0x12,
	0x1e, 0x0a, 0x0a, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x55, 0x55, 0x49, 0x44, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x0a, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x55, 0x55, 0x49, 0x44, 0x12,
	0x30, 0x0a, 0x13, 0x65, 0x78, 0x70, 0x69, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x54, 0x69, 0x6d,
	0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x03, 0x20, 0x01, 0x28, 0x05, 0x52, 0x13, 0x65, 0x78,
	0x70, 0x69, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d,
	0x70, 0x12, 0x30, 0x0a, 0x13, 0x69, 0x6e, 0x61, 0x63, 0x74, 0x69, 0x76, 0x69, 0x74, 0x79, 0x54,
	0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x04, 0x20, 0x01, 0x28, 0x05, 0x52, 0x13,
	0x69, 0x6e, 0x61, 0x63, 0x74, 0x69, 0x76, 0x69, 0x74, 0x79, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74,
	0x61, 0x6d, 0x70, 0x22, 0x12, 0x0a, 0x10, 0x4b, 0x65, 0x65, 0x70, 0x41, 0x6c, 0x69, 0x76, 0x65,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x22, 0x13, 0x0a, 0x11, 0x4b, 0x65, 0x65, 0x70, 0x41,
	0x6c, 0x69, 0x76, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x15, 0x0a, 0x13,
	0x43, 0x6c, 0x6f, 0x73, 0x65, 0x53, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x22, 0x16, 0x0a, 0x14, 0x43, 0x6c, 0x6f, 0x73, 0x65, 0x53, 0x65, 0x73, 0x73,
	0x69, 0x6f, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x32, 0xc6, 0x03, 0x0a, 0x14,
	0x41, 0x75, 0x74, 0x68, 0x6f, 0x72, 0x69, 0x7a, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x53, 0x65, 0x72,
	0x76, 0x69, 0x63, 0x65, 0x12, 0x8c, 0x01, 0x0a, 0x0b, 0x4f, 0x70, 0x65, 0x6e, 0x53, 0x65, 0x73,
	0x73, 0x69, 0x6f, 0x6e, 0x12, 0x20, 0x2e, 0x69, 0x6d, 0x6d, 0x75, 0x64, 0x62, 0x2e, 0x6d, 0x6f,
	0x64, 0x65, 0x6c, 0x2e, 0x4f, 0x70, 0x65, 0x6e, 0x53, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x21, 0x2e, 0x69, 0x6d, 0x6d, 0x75, 0x64, 0x62, 0x2e,
	0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x4f, 0x70, 0x65, 0x6e, 0x53, 0x65, 0x73, 0x73, 0x69, 0x6f,
	0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x38, 0x82, 0xd3, 0xe4, 0x93, 0x02,
	0x20, 0x22, 0x1b, 0x2f, 0x61, 0x75, 0x74, 0x68, 0x6f, 0x72, 0x69, 0x7a, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x2f, 0x73, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x2f, 0x6f, 0x70, 0x65, 0x6e, 0x3a, 0x01,
	0x2a, 0x92, 0x41, 0x0f, 0x0a, 0x0d, 0x61, 0x75, 0x74, 0x68, 0x6f, 0x72, 0x69, 0x7a, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x12, 0x8b, 0x01, 0x0a, 0x09, 0x4b, 0x65, 0x65, 0x70, 0x41, 0x6c, 0x69, 0x76,
	0x65, 0x12, 0x1e, 0x2e, 0x69, 0x6d, 0x6d, 0x75, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c,
	0x2e, 0x4b, 0x65, 0x65, 0x70, 0x41, 0x6c, 0x69, 0x76, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x1a, 0x1f, 0x2e, 0x69, 0x6d, 0x6d, 0x75, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c,
	0x2e, 0x4b, 0x65, 0x65, 0x70, 0x41, 0x6c, 0x69, 0x76, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x22, 0x3d, 0x82, 0xd3, 0xe4, 0x93, 0x02, 0x25, 0x22, 0x20, 0x2f, 0x61, 0x75, 0x74,
	0x68, 0x6f, 0x72, 0x69, 0x7a, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x73, 0x65, 0x73, 0x73, 0x69,
	0x6f, 0x6e, 0x2f, 0x6b, 0x65, 0x65, 0x70, 0x61, 0x6c, 0x69, 0x76, 0x65, 0x3a, 0x01, 0x2a, 0x92,
	0x41, 0x0f, 0x0a, 0x0d, 0x61, 0x75, 0x74, 0x68, 0x6f, 0x72, 0x69, 0x7a, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x12, 0x90, 0x01, 0x0a, 0x0c, 0x43, 0x6c, 0x6f, 0x73, 0x65, 0x53, 0x65, 0x73, 0x73, 0x69,
	0x6f, 0x6e, 0x12, 0x21, 0x2e, 0x69, 0x6d, 0x6d, 0x75, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65,
	0x6c, 0x2e, 0x43, 0x6c, 0x6f, 0x73, 0x65, 0x53, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x22, 0x2e, 0x69, 0x6d, 0x6d, 0x75, 0x64, 0x62, 0x2e, 0x6d,
	0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x43, 0x6c, 0x6f, 0x73, 0x65, 0x53, 0x65, 0x73, 0x73, 0x69, 0x6f,
	0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x39, 0x82, 0xd3, 0xe4, 0x93, 0x02,
	0x21, 0x22, 0x1c, 0x2f, 0x61, 0x75, 0x74, 0x68, 0x6f, 0x72, 0x69, 0x7a, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x2f, 0x73, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x2f, 0x63, 0x6c, 0x6f, 0x73, 0x65, 0x3a,
	0x01, 0x2a, 0x92, 0x41, 0x0f, 0x0a, 0x0d, 0x61, 0x75, 0x74, 0x68, 0x6f, 0x72, 0x69, 0x7a, 0x61,
	0x74, 0x69, 0x6f, 0x6e, 0x42, 0x66, 0x5a, 0x2f, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63,
	0x6f, 0x6d, 0x2f, 0x63, 0x6f, 0x64, 0x65, 0x6e, 0x6f, 0x74, 0x61, 0x72, 0x79, 0x2f, 0x69, 0x6d,
	0x6d, 0x75, 0x64, 0x62, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x92, 0x41, 0x32, 0x12, 0x27, 0x0a, 0x12, 0x69, 0x6d,
	0x6d, 0x75, 0x64, 0x62, 0x20, 0x52, 0x45, 0x53, 0x54, 0x20, 0x41, 0x50, 0x49, 0x20, 0x76, 0x32,
	0x12, 0x11, 0x41, 0x75, 0x74, 0x68, 0x6f, 0x72, 0x69, 0x7a, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x20,
	0x41, 0x50, 0x49, 0x22, 0x07, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x76, 0x32, 0x62, 0x06, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_authorization_proto_rawDescOnce sync.Once
	file_authorization_proto_rawDescData = file_authorization_proto_rawDesc
)

func file_authorization_proto_rawDescGZIP() []byte {
	file_authorization_proto_rawDescOnce.Do(func() {
		file_authorization_proto_rawDescData = protoimpl.X.CompressGZIP(file_authorization_proto_rawDescData)
	})
	return file_authorization_proto_rawDescData
}

var file_authorization_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_authorization_proto_goTypes = []interface{}{
	(*OpenSessionRequest)(nil),   // 0: immudb.model.OpenSessionRequest
	(*OpenSessionResponse)(nil),  // 1: immudb.model.OpenSessionResponse
	(*KeepAliveRequest)(nil),     // 2: immudb.model.KeepAliveRequest
	(*KeepAliveResponse)(nil),    // 3: immudb.model.KeepAliveResponse
	(*CloseSessionRequest)(nil),  // 4: immudb.model.CloseSessionRequest
	(*CloseSessionResponse)(nil), // 5: immudb.model.CloseSessionResponse
}
var file_authorization_proto_depIdxs = []int32{
	0, // 0: immudb.model.AuthorizationService.OpenSession:input_type -> immudb.model.OpenSessionRequest
	2, // 1: immudb.model.AuthorizationService.KeepAlive:input_type -> immudb.model.KeepAliveRequest
	4, // 2: immudb.model.AuthorizationService.CloseSession:input_type -> immudb.model.CloseSessionRequest
	1, // 3: immudb.model.AuthorizationService.OpenSession:output_type -> immudb.model.OpenSessionResponse
	3, // 4: immudb.model.AuthorizationService.KeepAlive:output_type -> immudb.model.KeepAliveResponse
	5, // 5: immudb.model.AuthorizationService.CloseSession:output_type -> immudb.model.CloseSessionResponse
	3, // [3:6] is the sub-list for method output_type
	0, // [0:3] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_authorization_proto_init() }
func file_authorization_proto_init() {
	if File_authorization_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_authorization_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*OpenSessionRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_authorization_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*OpenSessionResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_authorization_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*KeepAliveRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_authorization_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*KeepAliveResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_authorization_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CloseSessionRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_authorization_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CloseSessionResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_authorization_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_authorization_proto_goTypes,
		DependencyIndexes: file_authorization_proto_depIdxs,
		MessageInfos:      file_authorization_proto_msgTypes,
	}.Build()
	File_authorization_proto = out.File
	file_authorization_proto_rawDesc = nil
	file_authorization_proto_goTypes = nil
	file_authorization_proto_depIdxs = nil
}
