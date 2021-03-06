// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: remote.proto

package invertedindex

import (
	fmt "fmt"
	io "io"
	math "math"
	math_bits "math/bits"

	_ "github.com/gogo/protobuf/gogoproto"
	proto "github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/prompb"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

type StreamMetric struct {
	Labels               []prompb.Label `protobuf:"bytes,1,rep,name=Labels,proto3" json:"Labels"`
	StreamID             StreamID       `protobuf:"varint,2,opt,name=StreamID,proto3,customtype=StreamID" json:"StreamID"`
	XXX_NoUnkeyedLiteral struct{}       `json:"-"`
	XXX_unrecognized     []byte         `json:"-"`
	XXX_sizecache        int32          `json:"-"`
}

func (m *StreamMetric) Reset()         { *m = StreamMetric{} }
func (m *StreamMetric) String() string { return proto.CompactTextString(m) }
func (*StreamMetric) ProtoMessage()    {}
func (*StreamMetric) Descriptor() ([]byte, []int) {
	return fileDescriptor_eefc82927d57d89b, []int{0}
}
func (m *StreamMetric) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *StreamMetric) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_StreamMetric.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *StreamMetric) XXX_Merge(src proto.Message) {
	xxx_messageInfo_StreamMetric.Merge(m, src)
}
func (m *StreamMetric) XXX_Size() int {
	return m.Size()
}
func (m *StreamMetric) XXX_DiscardUnknown() {
	xxx_messageInfo_StreamMetric.DiscardUnknown(m)
}

var xxx_messageInfo_StreamMetric proto.InternalMessageInfo

func (m *StreamMetric) GetLabels() []prompb.Label {
	if m != nil {
		return m.Labels
	}
	return nil
}

func init() {
	proto.RegisterType((*StreamMetric)(nil), "yatsdb.StreamMetric")
}

func init() { proto.RegisterFile("remote.proto", fileDescriptor_eefc82927d57d89b) }

var fileDescriptor_eefc82927d57d89b = []byte{
	// 190 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x29, 0x4a, 0xcd, 0xcd,
	0x2f, 0x49, 0xd5, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0xab, 0x4c, 0x2c, 0x29, 0x4e, 0x49,
	0x92, 0x12, 0x2a, 0x28, 0xca, 0xcf, 0x2d, 0x48, 0xd2, 0x2f, 0xa9, 0x2c, 0x48, 0x2d, 0x86, 0xc8,
	0x49, 0x89, 0xa4, 0xe7, 0xa7, 0xe7, 0x83, 0x99, 0xfa, 0x20, 0x16, 0x44, 0x54, 0x29, 0x97, 0x8b,
	0x27, 0xb8, 0xa4, 0x28, 0x35, 0x31, 0xd7, 0x37, 0xb5, 0xa4, 0x28, 0x33, 0x59, 0x48, 0x9f, 0x8b,
	0xcd, 0x27, 0x31, 0x29, 0x35, 0xa7, 0x58, 0x82, 0x51, 0x81, 0x59, 0x83, 0xdb, 0x48, 0x10, 0xa4,
	0x2e, 0x37, 0xb5, 0x24, 0x23, 0xb5, 0xb4, 0x58, 0x0f, 0x2c, 0xe3, 0xc4, 0x72, 0xe2, 0x9e, 0x3c,
	0x43, 0x10, 0x54, 0x99, 0x90, 0x0e, 0x17, 0x07, 0xc4, 0x00, 0x4f, 0x17, 0x09, 0x26, 0x05, 0x46,
	0x0d, 0x16, 0x27, 0x01, 0x90, 0xfc, 0xad, 0x7b, 0xf2, 0x70, 0xf1, 0x20, 0x38, 0xcb, 0x49, 0xfa,
	0xc4, 0x23, 0x39, 0xc6, 0x0b, 0x8f, 0xe4, 0x18, 0x1f, 0x3c, 0x92, 0x63, 0x8c, 0xe2, 0xcd, 0xcc,
	0x2b, 0x4b, 0x2d, 0x2a, 0x49, 0x4d, 0xc9, 0xcc, 0x4b, 0x49, 0xad, 0x48, 0x62, 0x03, 0x3b, 0xc9,
	0x18, 0x10, 0x00, 0x00, 0xff, 0xff, 0x1f, 0x6d, 0x70, 0x4a, 0xd4, 0x00, 0x00, 0x00,
}

func (m *StreamMetric) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *StreamMetric) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *StreamMetric) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.XXX_unrecognized != nil {
		i -= len(m.XXX_unrecognized)
		copy(dAtA[i:], m.XXX_unrecognized)
	}
	if m.StreamID != 0 {
		i = encodeVarintRemote(dAtA, i, uint64(m.StreamID))
		i--
		dAtA[i] = 0x10
	}
	if len(m.Labels) > 0 {
		for iNdEx := len(m.Labels) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Labels[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintRemote(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0xa
		}
	}
	return len(dAtA) - i, nil
}

func encodeVarintRemote(dAtA []byte, offset int, v uint64) int {
	offset -= sovRemote(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *StreamMetric) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if len(m.Labels) > 0 {
		for _, e := range m.Labels {
			l = e.Size()
			n += 1 + l + sovRemote(uint64(l))
		}
	}
	if m.StreamID != 0 {
		n += 1 + sovRemote(uint64(m.StreamID))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func sovRemote(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozRemote(x uint64) (n int) {
	return sovRemote(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *StreamMetric) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowRemote
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: StreamMetric: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: StreamMetric: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Labels", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRemote
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthRemote
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthRemote
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Labels = append(m.Labels, prompb.Label{})
			if err := m.Labels[len(m.Labels)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field StreamID", wireType)
			}
			m.StreamID = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRemote
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.StreamID |= StreamID(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipRemote(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthRemote
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipRemote(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowRemote
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowRemote
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowRemote
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthRemote
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupRemote
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthRemote
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthRemote        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowRemote          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupRemote = fmt.Errorf("proto: unexpected end of group")
)
