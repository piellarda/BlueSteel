//
//  Schema.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

public enum AvroType {
    // Primitives
    case aNull
    case aBoolean
    case aInt
    case aLong
    case aFloat
    case aDouble
    case aString
    case aBytes

    // Complex
    case aEnum
    case aFixed
    case aRecord
    case aArray
    case aMap

    // Invalid
    case aInvalidType

    init(_ typeString: String) {

        if typeString == "boolean" {
            self = .aBoolean
        } else if typeString == "int" {
            self = .aInt
        } else if typeString == "long" {
            self = .aLong
        } else if typeString == "float"  {
            self = .aFloat
        } else if typeString == "double" {
            self = .aDouble
        } else if typeString == "string" {
            self = .aString
        } else if typeString == "bytes" {
            self = .aBytes
        } else if typeString == "enum" {
            self = .aEnum
        } else if typeString == "fixed" {
            self = .aFixed
        } else if typeString == "record" {
            self = .aRecord
        } else if typeString == "array" {
            self = .aArray
        } else if typeString == "map" {
            self = .aMap
        } else if typeString == "null" {
            self = .aNull
        } else {
            self = .aInvalidType
        }
        return
    }
}

public enum Schema {
    case avroNullSchema
    case avroBooleanSchema
    case avroIntSchema
    case avroLongSchema
    case avroFloatSchema
    case avroDoubleSchema
    case avroStringSchema
    case avroBytesSchema

    case avroArraySchema(Box<Schema>)
    case avroMapSchema(Box<Schema>)
    case avroUnionSchema(Array<Schema>)

    // Named Types
    case avroFixedSchema(String, Int)
    case avroEnumSchema(String, Array<String>)
    case avroRecordSchema(String, Array<Schema>)
    case avroFieldSchema(String, Box<Schema>)

    // TODO: Report errors for invalid schemas.
    case avroInvalidSchema

    static func assembleFullName(_ namespace:String?, name: String) -> String {

        if name.range(of: ".") == nil {
            if let space = namespace {
                return space + "." + name
            }
        }
        return name

    }

    @discardableResult
    public func parsingCanonicalForm(_ existingTypes: inout [String])
        -> String?
    {
        switch self {
        case .avroNullSchema :
            return "\"null\""
        case .avroBooleanSchema :
            return "\"boolean\""
        case .avroIntSchema :
            return "\"int\""
        case .avroLongSchema :
            return "\"long\""
        case .avroFloatSchema :
            return "\"float\""
        case .avroDoubleSchema :
            return "\"double\""
        case .avroStringSchema :
            return "\"string\""
        case .avroBytesSchema :
            return "\"bytes\""


        case .avroArraySchema(let boxed) :
            if let arrayPCF = boxed.value.parsingCanonicalForm(&existingTypes) {
                return "{\"type\":\"array\",\"items\":" + arrayPCF + "}"
            } else {
                return nil
            }

        case .avroMapSchema(let boxed) :
            if let mapPCF = boxed.value.parsingCanonicalForm(&existingTypes) {
                return "{\"type\":\"map\",\"values\":" + mapPCF + "}"
            } else {
                return nil
            }

        case .avroEnumSchema(let name, let enumValues) :
            if existingTypes.firstIndex(of: name) != nil {
                return "\"" + name + "\""
            } else {
                existingTypes.append(name)
                var str = "{\"name\":\"" + name + "\",\"type\":\"enum\",\"symbols\":["
                var first = true
                for val in enumValues {
                    if first {
                        str += "\"\(val)\""
                        first = false
                    } else {
                        str += ",\"\(val)\""
                    }
                }
                str += "]}"
                return str
            }

        case .avroRecordSchema(let name, let fields) :
            if existingTypes.firstIndex(of: name) != nil {
                return "\"" + name + "\""
            } else {
                existingTypes.append(name)
                var str = "{\"name\":\"" + name + "\",\"type\":\"record\",\"fields\":["
                var first = true
                for field in fields {
                    if !first {
                        str += ","
                    } else {
                        first = false
                    }

                    switch field {
                    case .avroFieldSchema(let fieldName, let fieldType) :
                        if let fieldPCF = fieldType.value.parsingCanonicalForm(&existingTypes) {
                            str += "{\"name\":\"" + fieldName + "\",\"type\":" + fieldPCF + "}"
                        } else {
                            print(fieldName)
                            return nil
                        }
                    default :
                        return nil
                    }
                }
                str += "]}"
                return str
            }

        case .avroFixedSchema(let name, let size) :
            if existingTypes.firstIndex(of: name) != nil {
                return "\"" + name + "\""
            } else {
                existingTypes.append(name)
                return "{\"name\":\"" + name + "\",\"type\":\"fixed\",\"size\":\(size)}"
            }

        case .avroUnionSchema(let unionSchemas) :
            var str = "["
            var first = true
            for uschema in unionSchemas {
                if !first {
                    str += ","
                } else {
                    first = false
                }

                if let unionPCF = uschema.parsingCanonicalForm(&existingTypes) {
                    str += unionPCF
                } else {
                    return nil
                }
            }
            str += "]"
            return str
        default :
            return nil
        }
    }
    
    func json() -> String? {
        var etypes: [String] = []
        return parsingCanonicalForm(&etypes)
    }

    public func fingerprint() -> [UInt8]? {
        var etypes: [String] = []
        if let pcf = self.parsingCanonicalForm(&etypes) {
            var hash = [UInt8](repeating: 0, count: 32)
            if let cString = pcf.cString(using: String.Encoding.utf8) {
                // Compute hash of PCF string without the NULL terminator.

                BlueSteel_SHA256(cString, UInt32(cString.count - 1), &hash)
                return hash
            }
        }
        return nil
    }

    public init?(_ json: Data)
    {
        var cached: [String:Schema] = [:]

        guard let jsonObject = (try? JSONSerialization.jsonObject(with: json, options: [])) as? [String: Any] else { return nil }

        self = Schema(jsonObject, typeKey:"type", namespace: nil, cachedSchemas: &cached)
    }

    public init?(_ json: String) {
        guard let schemaData = json.data(using: String.Encoding.utf8, allowLossyConversion: false) else { return nil }
        
        self.init(schemaData)
    }

    init(_ json: [String: Any], typeKey key: String, namespace ns: String?, cachedSchemas cache: inout [String:Schema]) {
        var schemaNamespace: String?
        if let jsonNamespace = json["namespace"] as? String {
            schemaNamespace = jsonNamespace
        } else {
            schemaNamespace = ns
        }

        if let typeString = json[key] as? String {
            // FIXME: Converting to AvroType, and then switching, is double the work.
            let avroType = AvroType(typeString)

            switch avroType {
            case .aBoolean :
                self = .avroBooleanSchema
            case .aInt :
                self = .avroIntSchema
            case .aLong :
                self = .avroLongSchema
            case .aFloat :
                self = .avroFloatSchema
            case .aDouble :
                self = .avroDoubleSchema
            case .aString :
                self = .avroStringSchema
            case .aNull :
                self = .avroNullSchema
            case .aBytes :
                self = .avroBytesSchema

            case .aMap :
                let schema = Schema(json, typeKey: "values", namespace: schemaNamespace, cachedSchemas: &cache)

                switch schema {
                case .avroInvalidSchema :
                    self = .avroInvalidSchema
                default :
                    self = .avroMapSchema(Box(schema))
                }

            case .aArray :
                let schema = Schema(json, typeKey: "items", namespace: schemaNamespace, cachedSchemas: &cache)

                switch schema {
                case .avroInvalidSchema :
                    self = .avroInvalidSchema
                default :
                    self = .avroArraySchema(Box(schema))
                }

            case .aRecord :
                // Records must be named
                if let recordName = json["name"] as? String {
                    let fullRecordName = Schema.assembleFullName(schemaNamespace, name: recordName)

                    if let fields = json["fields"] as? [[String: Any]] {
                        var recordFields: [Schema] = []

                        for field in fields {
                            // Fields must be named
                            if let fieldName = field["name"] as? String {
                                let schema = Schema(field, typeKey: "type", namespace: schemaNamespace, cachedSchemas: &cache)

                                switch schema {
                                case .avroInvalidSchema :
                                    self = .avroInvalidSchema
                                    return

                                default :
                                    recordFields.append(.avroFieldSchema(fieldName, Box(schema)))
                                }
                            }
                            else {
                                self = .avroInvalidSchema
                                return
                            }
                        }
                        self = .avroRecordSchema(fullRecordName, recordFields)
                        cache[fullRecordName] = self
                    }
                    else {
                        self = .avroInvalidSchema
                    }
                }
                else {
                    self = .avroInvalidSchema
                }

            case .aEnum :
                if let enumName = json["name"] as? String {
                    if let symbols = json["symbols"] as? [Any] {
                        var symbolStrings: [String] = []
                        for sym in symbols {
                            if let symbol = sym as? String {
                                symbolStrings.append(symbol)
                            } else {
                                self = .avroInvalidSchema
                                return
                            }
                        }

                        let fullEnumName = Schema.assembleFullName(schemaNamespace, name: enumName)

                        self = .avroEnumSchema(fullEnumName, symbolStrings)
                        cache[fullEnumName] = self
                    }
                    else {
                        self = .avroInvalidSchema
                    }
                }
                else {
                    self = .avroInvalidSchema
                }

            case .aFixed :
                if let fixedName = json["name"] as? String {
                    if let size = json["size"] as? Int {
                        let fullFixedName = Schema.assembleFullName(schemaNamespace, name: fixedName)
                        self = .avroFixedSchema(fullFixedName, size)
                        cache[fullFixedName] = self
                        return
                    }
                }
                self = .avroInvalidSchema

            default:
                // Schema type is invalid
                let fullTypeName = Schema.assembleFullName(schemaNamespace, name: typeString)

                if let cachedSchema = cache[fullTypeName] {
                    self = cachedSchema
                } else {
                    self = .avroInvalidSchema
                }
            }

        }
        else if let dict = json[key] as? [String: Any] {
            self = Schema(dict, typeKey: "type", namespace: schemaNamespace, cachedSchemas: &cache)
        }
        else if let unionSchema = json[key] as? [Any] {
            // Union
            var schemas: [Schema] = []
            for def in unionSchema {
                var schema: Schema = .avroInvalidSchema
                if let typeString = def as? String {
                    let fullTypeName = Schema.assembleFullName(schemaNamespace, name: typeString)
                    let avroType = AvroType(typeString)

                    if  avroType != .aInvalidType {
                        //schema = .PrimitiveSchema(avroType)
                        switch avroType {
                        case .aBoolean :
                            schema = .avroBooleanSchema
                        case .aInt :
                            schema = .avroIntSchema
                        case .aLong :
                            schema = .avroLongSchema
                        case .aFloat :
                            schema = .avroFloatSchema
                        case .aDouble :
                            schema = .avroDoubleSchema
                        case .aString :
                            schema = .avroStringSchema
                        case .aNull :
                            schema = .avroNullSchema
                        case .aBytes :
                            schema = .avroBytesSchema
                        default :
                            schema = .avroInvalidSchema
                        }
                    } else if let cachedSchema = cache[fullTypeName] {
                        schema = cachedSchema
                    } else {
                        schema = .avroInvalidSchema
                    }

                }
                else if let dict = def as? [String: Any] {
                    schema = Schema(dict, typeKey: "type", namespace: schemaNamespace, cachedSchemas: &cache)
                }
                else {
                    schema = .avroInvalidSchema
                }

                switch schema {
                case .avroInvalidSchema:
                    self = .avroInvalidSchema
                    return
                default:
                    schemas.append(schema)
                }
            }
            self = .avroUnionSchema(schemas)
        } else {
            self = .avroInvalidSchema
        }
    }
}

extension Schema : Equatable {

}

public func ==(lhs: Schema, rhs: Schema) -> Bool {
    switch (lhs) {
    case .avroBooleanSchema :
        switch rhs {
        case .avroBooleanSchema :
            return true
        default :
            return false
        }

    case .avroIntSchema :
        switch rhs {
        case .avroIntSchema :
            return true
        default :
            return false
        }
    case .avroLongSchema :
        switch rhs {
        case .avroLongSchema :
            return true
        default :
            return false
        }

    case .avroFloatSchema :
        switch rhs {
        case .avroFloatSchema :
            return true
        default :
            return false
        }

    case .avroDoubleSchema :
        switch rhs {
        case .avroDoubleSchema :
            return true
        default :
            return false
        }

    case .avroBytesSchema :
        switch rhs {
        case .avroBytesSchema :
            return true
        default :
            return false
        }

    case .avroStringSchema :
        switch rhs {
        case .avroStringSchema :
            return true
        default :
            return false
        }

    case .avroNullSchema :
        switch rhs {
        case .avroNullSchema :
            return true
        default :
            return false
        }

    case .avroArraySchema(let lbox) :
        switch rhs {
        case .avroArraySchema(let rbox) :
            if lbox.value == rbox.value {
                return true
            }
            return false
        default :
            return false
        }

    case .avroMapSchema(let lbox) :
        switch rhs {
        case .avroMapSchema(let rbox) :
            if lbox.value == rbox.value {
                return true
            }
            return false
        default :
            return false
        }

    case .avroRecordSchema(let lRecordName, let lRecordSchemas) :
        switch rhs {
        case .avroRecordSchema(let rRecordName, let rRecordSchemas) :
            if (lRecordName == rRecordName) && (lRecordSchemas.count == rRecordSchemas.count) {
                for idx in 0..<lRecordSchemas.count {
                    if lRecordSchemas[idx] != rRecordSchemas[idx] {
                        return false
                    }
                }
                return true
            }
            return false
        default :
            return false
        }

    case .avroFieldSchema(let lFieldName, let lFieldBox) :
        switch rhs {
        case .avroFieldSchema(let rFieldName, let rFieldBox) :
            if (lFieldName == rFieldName) && (lFieldBox.value == rFieldBox.value) {
                return true
            }
            return false
        default :
            return false
        }

    case .avroUnionSchema(let lUnionSchemas) :
        switch rhs {
        case .avroUnionSchema(let rUnionSchemas) :
            if lUnionSchemas.count == rUnionSchemas.count {
                for idx in 0..<lUnionSchemas.count {
                    if lUnionSchemas[idx] != rUnionSchemas[idx] {
                        return false
                    }
                }
                return true
            }
            return false
        default :
            return false
        }

    case .avroEnumSchema(let lEnumName, let lSymbols) :
        switch rhs {
        case .avroEnumSchema(let rEnumName, let rSymbols) :
            if (lEnumName == rEnumName) && (lSymbols.count == rSymbols.count) {
                for idx in 0..<lSymbols.count {
                    if lSymbols[idx] != rSymbols[idx] {
                        return false
                    }
                }
                return true
            }
            return false
        default :
            return false
        }

    case .avroFixedSchema(let lFixedName, let lSize) :
        switch rhs {
        case .avroFixedSchema(let rFixedName, let rSize) :
            if (lFixedName == rFixedName) && (lSize == rSize) {
                return true
            }
            return false
        default :
            return false
        }

    default :
        return false
    }
}
