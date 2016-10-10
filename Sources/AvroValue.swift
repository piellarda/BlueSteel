//
//  AvroValue.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation


extension Dictionary {

}

public func +=<K, V>(left: inout Dictionary<K, V>, right: Dictionary<K, V>)
{
    for (k, v) in right {
        left.updateValue(v, forKey: k)
    }
}

public enum AvroValue {
    // Primitives
    case avroNullValue
    case avroBooleanValue(Bool)
    case avroIntValue(Int32)
    case avroLongValue(Int64)
    case avroFloatValue(Float)
    case avroDoubleValue(Double)
    case avroBytesValue([UInt8])
    case avroStringValue(String)

    // Complex Types
    case avroArrayValue([AvroValue])
    case avroMapValue(Dictionary<String, AvroValue>)
    case avroUnionValue(Int, Box<AvroValue>)
    case avroRecordValue(Dictionary<String, AvroValue>)
    case avroEnumValue(Int, String)
    case avroFixedValue([UInt8])

    case avroInvalidValue

    public var boolean: Bool? {
        switch self {
        case .avroBooleanValue(let value) :
            return value
        case .avroUnionValue(_, let box) :
            return box.value.boolean
        default :
            return nil
        }
    }

    public var string: String? {
        switch self {
        case .avroStringValue(let value) :
            return value
        case .avroUnionValue(_, let box) :
            return box.value.string
        default :
            return nil
        }
    }

    public var integer: Int32? {
        switch self {
        case .avroIntValue(let value) :
            return value
        case .avroUnionValue(_, let box) :
            return box.value.integer
        default :
            return nil
        }
    }

    public var long: Int64? {
        switch self {
        case .avroLongValue(let value) :
            return value
        case .avroUnionValue(_, let box) :
            return box.value.long
        default :
            return nil
        }
    }

    public var float: Float? {
        switch self {
        case .avroFloatValue(let value) :
            return value
        case .avroUnionValue(_, let box) :
            return box.value.float
        default :
            return nil
        }
    }

    public var double: Double? {
        switch self {
        case .avroDoubleValue(let value) :
            return value
        case .avroUnionValue(_, let box) :
            return box.value.double
        default :
            return nil
        }
    }

    public var bytes: [UInt8]? {
        switch self {
        case .avroBytesValue(let value) :
            return value
        case .avroUnionValue(_, let box) :
            return box.value.bytes
        default :
            return nil
        }
    }

    public var array: Array<AvroValue>? {
        switch self {
        case .avroArrayValue(let values) :
            return values
        case .avroUnionValue(_, let box) :
            return box.value.array
        default :
            return nil
        }
    }

    public var map: Dictionary<String, AvroValue>? {
        switch self {
        case .avroMapValue(let pairs) :
            return pairs
        case .avroUnionValue(_, let box) :
            return box.value.map
        default :
            return nil
        }
    }

    public var record: Dictionary<String, AvroValue>? {
        switch self {
        case .avroRecordValue(let fields) :
            return fields
        case .avroUnionValue(_, let box) :
            return box.value.record
        default :
            return nil
        }
    }

    public var enumeration: String? {
        switch self {
        case .avroEnumValue(_, let value) :
            return value
        case .avroUnionValue(_, let box) :
            return box.value.enumeration
        default :
            return nil
        }
    }

    public var enumerationRawValue: Int? {
        switch self {
        case .avroEnumValue(let index, _ ):
            return index
        case .avroUnionValue(_, let box) :
            return box.value.enumerationRawValue
        default :
            return nil
        }
    }

    public var fixed: [UInt8]? {
        switch self {
        case .avroFixedValue(let bytes) :
            return bytes
        case .avroUnionValue(_, let box) :
            return box.value.fixed
        default :
            return nil
        }
    }

    static func decodeArrayBlock(_ schema: Schema, count:Int64, decoder: AvroDecoder) -> [AvroValue]? {
        var values: [AvroValue] = []
        for _ in 0...count - 1 {
            let value = AvroValue(schema, withDecoder: decoder)
            switch value {
            case .avroInvalidValue :
                return nil
            default :
                values.append(value)
            }
        }
        return values
    }

    static func decodeMapBlock(_ schema: Schema, count:Int64, decoder: AvroDecoder) -> Dictionary<String, AvroValue>? {
        var pairs: Dictionary<String, AvroValue> = Dictionary()
        for _ in 0...count - 1 {
            if let key = AvroValue(.avroStringSchema, withDecoder: decoder).string {
                let value = AvroValue(schema, withDecoder: decoder)
                switch value {
                case .avroInvalidValue :
                    return nil
                default :
                    pairs[key] = value
                }
            } else {
                return nil
            }
        }
        return pairs
    }

    public func encode(_ encoder: AvroEncoder) -> [UInt8]? {
        switch self {
        case .avroNullValue :
            encoder.encodeNull()
        case .avroBooleanValue(let value) :
            encoder.encodeBoolean(value)
        case .avroIntValue(let value) :
            encoder.encodeInt(value)
        case .avroLongValue(let value) :
            encoder.encodeLong(value)
        case .avroFloatValue(let value) :
            encoder.encodeFloat(value)
        case .avroDoubleValue(let value) :
            encoder.encodeDouble(value)
        case .avroStringValue(let value) :
            encoder.encodeString(value)
        case .avroBytesValue(let value) :
            encoder.encodeBytes(value)
        case .avroFixedValue(let value) :
            encoder.encodeFixed(value)

        case .avroArrayValue(let values) :
            encoder.encodeLong(Int64(values.count))
            for value in values {
                _ = value.encode(encoder)
            }
            encoder.encodeLong(0)

        case .avroMapValue(let pairs) :
            encoder.encodeLong(Int64(pairs.count))
            for key in pairs.keys {
                encoder.encodeString(key)
                if let value = pairs[key] {
                    _ = value.encode(encoder)
                } else {
                    return nil
                }
            }

        case .avroRecordValue(let pairs) :
            for key in pairs.keys {
                encoder.encodeString(key)
                if let value = pairs[key] {
                    _ = value.encode(encoder)
                } else {
                    return nil
                }
            }

        case .avroEnumValue(let index, _) :
            encoder.encodeInt(Int32(index))

        case .avroUnionValue(let index, let box) :
            encoder.encodeLong(Int64(index))
            _ = box.value.encode(encoder)
        default :
            return nil
        }
        return encoder.bytes
    }

    public init(jsonSchema: String, withBytes bytes: [UInt8]) {
        let avroData = Data(bytes: UnsafeRawPointer(bytes), count: bytes.count)

        self.init(jsonSchema: jsonSchema, withData: avroData)
    }

    public init(jsonSchema: String, withData data: Data) {
        let schema = Schema(jsonSchema)

        self.init(schema: schema, withData: data)
    }

    public init(schema: Schema, withBytes bytes: [UInt8]) {
        let avroData = Data(bytes: UnsafeRawPointer(bytes) , count: bytes.count)

        self.init(schema: schema, withData: avroData)
    }

    public init(schema: Schema, withData data: Data) {
        let decoder = AvroDecoder(data)

        self.init(schema, withDecoder: decoder)
    }

    init(_ schema: Schema, withDecoder decoder: AvroDecoder) {

        switch schema {
        case .avroNullSchema :
            self = .avroNullValue

        case .avroBooleanSchema :
            if let decoded = decoder.decodeBoolean() {
                self = .avroBooleanValue(decoded)
            } else {
                self = .avroInvalidValue
            }

        case .avroIntSchema :
            if let decoded = decoder.decodeInt() {
                self = .avroIntValue(decoded)
            } else {
                self = .avroInvalidValue
            }

        case .avroLongSchema :
            if let decoded = decoder.decodeLong() {
                self = .avroLongValue(decoded)
            } else {
                self = .avroInvalidValue
            }

        case .avroFloatSchema :
            if let decoded = decoder.decodeFloat() {
                self = .avroFloatValue(decoded)
            } else {
                self = .avroInvalidValue
            }

        case .avroDoubleSchema :
            if let decoded = decoder.decodeDouble() {
                self = .avroDoubleValue(decoded)
            } else {
                self = .avroInvalidValue
            }

        case .avroStringSchema :
            if let decoded = decoder.decodeString() {
                self = .avroStringValue(decoded)
            } else {
                self = .avroInvalidValue
            }

        case .avroBytesSchema :
            if let decoded = decoder.decodeBytes() {
                self = .avroBytesValue(decoded)
            } else {
                self = .avroInvalidValue
            }

        // TODO: Collections negative count support.
        case .avroArraySchema(let boxedSchema) :
            var values: [AvroValue] = []
            while let count = decoder.decodeLong() {
                if count == 0 {
                    self = .avroArrayValue(values)
                    return
                }

                if let block = AvroValue.decodeArrayBlock(boxedSchema.value, count: count, decoder: decoder) {
                    values += block
                } else {
                    self = .avroInvalidValue
                    return
                }
            }
            self = .avroInvalidValue


        case .avroMapSchema(let boxedSchema) :
            var pairs: Dictionary<String, AvroValue> = [:]
            while let count = decoder.decodeLong() {
                if count == 0 {
                    self = .avroMapValue(pairs)
                    return
                }
                if let block = AvroValue.decodeMapBlock(boxedSchema.value, count: count, decoder: decoder) {
                    pairs += block
                } else {
                    self = .avroInvalidValue
                    return
                }
            }
            self = .avroInvalidValue


        case .avroEnumSchema(_, let enumValues) :
            if let index = decoder.decodeInt() {
                if (0..<enumValues.count).contains(Int(index)) {
                    self = .avroEnumValue(Int(index), enumValues[Int(index)])
                    return
                }
            }
            self = .avroInvalidValue

        case .avroRecordSchema(_, let fields) :
            var pairs: Dictionary<String, AvroValue> = [:]
            for field in fields {
                switch field {
                case .avroFieldSchema(let key, let box) :
                    pairs[key] = AvroValue(box.value, withDecoder: decoder)
                default :
                    self = .avroInvalidValue
                    return
                }
            }
            self = .avroRecordValue(pairs)

        case .avroFixedSchema(_, let size) :
            if let bytes = decoder.decodeFixed(size) {
                self = .avroFixedValue(bytes)
            } else {
                self = .avroInvalidValue
            }

        case .avroUnionSchema(let schemas) :
            if let index = decoder.decodeLong() {
                if Int(index) < schemas.count {
                    self = .avroUnionValue(Int(index), Box(AvroValue(schemas[Int(index)], withDecoder: decoder)))
                    return
                }
            }
            self = .avroInvalidValue

        default :
            self = .avroInvalidValue
        }
    }
}

extension AvroValue: ExpressibleByNilLiteral {
    public init(nilLiteral: ()) {
        self = .avroNullValue
    }
}

extension AvroValue: ExpressibleByBooleanLiteral {
    public init(booleanLiteral value: Bool) {
        self = .avroBooleanValue(value)
    }
}

extension AvroValue: ExpressibleByIntegerLiteral {
    public init(integerLiteral value: IntegerLiteralType) {
        self = .avroLongValue(Int64(value))
    }
}

extension AvroValue: ExpressibleByFloatLiteral {
    public init(floatLiteral value: FloatLiteralType) {
        self = .avroDoubleValue(value)
    }
}

extension AvroValue: ExpressibleByStringLiteral {

    public init(unicodeScalarLiteral value: String) {
        self = .avroInvalidValue
    }

    public init(extendedGraphemeClusterLiteral value: String) {
        self = .avroStringValue(value)
    }

    public init(stringLiteral value: String) {
        self = .avroStringValue(value)
    }
}

extension AvroValue: ExpressibleByArrayLiteral {
    public init(arrayLiteral elements: AvroValue...) {
        self = .avroArrayValue(elements)
    }
}

extension AvroValue: ExpressibleByDictionaryLiteral {

    public init(dictionaryLiteral elements:(String, AvroValue)...) {
        var tmp = [String:AvroValue](minimumCapacity: elements.count)
        for kv in elements {
            tmp[kv.0] = kv.1
        }
        self = .avroMapValue(tmp)
    }
}
