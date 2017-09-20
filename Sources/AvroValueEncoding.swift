//
//  AvroValueEncoding.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

/// Avro value binary encoding.
public extension AvroValue {

    /**
    AvroValue binary encoding.

    :param schemaData Avro JSON schema as a `Data` instance.

    :returns Avro binary encoding as byte array. Nil if encoding fails.
    */
    public func encode(_ schemaData: Data) -> [UInt8]? {
        guard let schema = Schema(schemaData) else { return nil }

        return self.encode(schema)
    }

    /**
    AvroValue binary encoding.

    :param jsonSchema Avro JSON schema string.

    :returns Avro binary encoding as byte array. Nil if encoding fails.
    */
    public func encode(_ jsonSchema: String) -> [UInt8]? {
        guard let schema = Schema(jsonSchema) else { return nil }

        return self.encode(schema)
    }

    /**
    AvroValue binary encoding.

    :param schema Avro schema object.

    :returns Avro binary encoding as byte array. Nil if encoding fails.
    */
    public func encode(_ schema: Schema) -> [UInt8]? {
        let encoder = AvroEncoder()
        return self.encode(encoder, schema: schema)
    }

    /**
    AvroValue binary encoding.

    :param encoder Avro Encoder.
    :param schema Avro schema object.

    :returns Avro binary encoding as byte array. Nil if encoding fails.
    */
    public func encode(_ encoder: AvroEncoder, schema: Schema) -> [UInt8]? {
        switch schema {

            case .avroNullSchema :
                encoder.encodeNull()

            case .avroBooleanSchema :
                switch self {
                case .avroBooleanValue(let value) :
                    encoder.encodeBoolean(value)
                default :
                    return nil
                }

            case .avroIntSchema :
                switch self {
                case .avroIntValue(let value) :
                    encoder.encodeInt(value)
                default :
                    return nil
                }

            case .avroLongSchema :
                switch self {
                case .avroIntValue(let value) :
                    encoder.encodeLong(Int64(value))
                case .avroLongValue(let value) :
                    encoder.encodeLong(value)
                default :
                    return nil
                }

            case .avroFloatSchema :
                switch self {
                case .avroIntValue(let value) :
                    encoder.encodeFloat(Float(value))
                case .avroLongValue(let value) :
                    encoder.encodeFloat(Float(value))
                case .avroFloatValue(let value) :
                    encoder.encodeFloat(value)
                default :
                    return nil
                }

            case .avroDoubleSchema :
                switch self {
                case .avroIntValue(let value) :
                    encoder.encodeDouble(Double(value))
                case .avroLongValue(let value) :
                    encoder.encodeDouble(Double(value))
                case .avroFloatValue(let value) :
                    encoder.encodeDouble(Double(value))
                case .avroDoubleValue(let value) :
                    encoder.encodeDouble(value)
                default :
                    return nil
                }

            case .avroStringSchema, .avroBytesSchema :
                switch self {
                case .avroStringValue(let value) :
                    encoder.encodeString(value)
                case .avroBytesValue(let value) :
                    encoder.encodeBytes(value)
                default :
                    return nil
                }

        case .avroArraySchema(let box) :
            switch self {
            case .avroArrayValue(let values) :
                if values.count != 0 {
                    encoder.encodeLong(Int64(values.count))
                    for value in values {
                        if value.encode(encoder, schema: box.value) == nil {
                            return nil
                        }
                    }
                }
                encoder.encodeLong(0)
            default :
                return nil
            }

        case .avroMapSchema(let box) :
            switch self {
            case .avroMapValue(let pairs) :
                if pairs.count != 0 {
                    encoder.encodeLong(Int64(pairs.count))
                    for key in pairs.keys {
                        encoder.encodeString(key)
                        if let value = pairs[key] {
                            if value.encode(encoder, schema: box.value) == nil {
                                return nil
                            }
                        } else {
                            return nil
                        }
                    }
                }
                encoder.encodeLong(0)
            default :
                return nil
            }

        case .avroRecordSchema(_, let fieldSchemas) :
            switch self {
            case .avroRecordValue(let pairs) :
                for fSchema in fieldSchemas {
                    switch fSchema {
                    case .avroFieldSchema(let key, let box) :
                        if let value = pairs[key] {
                            if value.encode(encoder, schema: box.value) == nil {
                                return nil
                            }
                        } else {
                            // Since we don't support schema defaults, fail encoding when values are missing for schema keys.
                            return nil
                        }
                    default :
                        return nil
                    }
                }
            default :
                return nil
            }

        case .avroEnumSchema(_, let enumValues):
            switch self {
            case .avroEnumValue(let index, let value) :
                if index >= enumValues.count || enumValues[index] != value {
                    return nil
                }
                encoder.encodeInt(Int32(index))
            default :
                return nil
            }

        case .avroUnionSchema(let uSchemas) :
            switch self {
            case .avroUnionValue(let index, let box) :
                encoder.encodeLong(Int64(index))
                if index < uSchemas.count {
                    if box.value.encode(encoder, schema: uSchemas[index]) == nil {
                        return nil
                    }
                } else {
                    return nil
                }
            default :
                return nil
            }


        case .avroFixedSchema(_, let size) :
            switch self {
            case .avroFixedValue(let fixedBytes) :
                if fixedBytes.count == size {
                    encoder.encodeFixed(fixedBytes)
                } else {
                    return nil
                }
            default :
                return nil
            }

        default :
            return nil
        }
        return encoder.bytes
    }
}
