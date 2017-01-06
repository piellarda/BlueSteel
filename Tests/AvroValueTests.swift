//
//  AvroValueTests.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import XCTest
import BlueSteel

class AvroValueTests: XCTestCase {

    override func setUp() {
        super.setUp()
    }

    override func tearDown() {
        super.tearDown()
    }

    func testStringValue() {
        let avroBytes: [UInt8] = [0x06, 0x66, 0x6f, 0x6f]
        let jsonSchema = "{ \"type\" : \"string\" }"
        let schema = Schema(jsonSchema)

        let value = AvroValue(schema: schema!, withBytes: avroBytes).string
        XCTAssertEqual(value, "foo", "Strings don't match.")
        
        let inputStream = InputStream(data: Data(bytes: avroBytes))
        inputStream.open()
        let valueFromStream = AvroValue(schema: schema!, withInputStream: inputStream).string
        XCTAssertEqual(valueFromStream, "foo", "Strings don't match.")
    }

    func testByteValue() {
        let avroBytes: [UInt8] = [0x06, 0x66, 0x6f, 0x6f]
        let jsonSchema = "{ \"type\" : \"bytes\" }"
        let schema = Schema(jsonSchema)

        if let value = AvroValue(schema: schema!, withBytes: avroBytes).bytes {
            XCTAssertEqual(value, [0x66, 0x6f, 0x6f], "Byte arrays don't match.")
        } else {
            XCTFail("Nil value")
        }
        
        let inputStream = InputStream(data: Data(bytes: avroBytes))
        inputStream.open()
        if let value = AvroValue(schema: schema!, withInputStream: inputStream).bytes {
            XCTAssertEqual(value, [0x66, 0x6f, 0x6f], "Byte arrays don't match.")
        } else {
            XCTFail("Nil value")
        }
    }

    func testIntValue() {
        let avroBytes: [UInt8] = [0x96, 0xde, 0x87, 0x3]
        let jsonSchema = "{ \"type\" : \"int\" }"
        let schema = Schema(jsonSchema)

        let value = AvroValue(jsonSchema: jsonSchema, withBytes: avroBytes).integer
        XCTAssertEqual(value, 3209099, "Integers don't match.")
        
        let inputStream = InputStream(data: Data(bytes: avroBytes))
        inputStream.open()
        let valueFromStream = AvroValue(schema: schema, withInputStream: inputStream).integer
        XCTAssertEqual(valueFromStream, 3209099, "Integers don't match.")
    }

    func testLongValue() {
        let avroBytes: [UInt8] = [0x96, 0xde, 0x87, 0x3]
        let jsonSchema = "{ \"type\" : \"long\" }"
        let schema = Schema(jsonSchema)

        let value = AvroValue(schema: schema, withBytes: avroBytes).long
        XCTAssertEqual(value, 3209099, "Longs don't match.")
        
        let inputStream = InputStream(data: Data(bytes: avroBytes))
        inputStream.open()
        let valueFromStream = AvroValue(schema: schema, withInputStream: inputStream).long
        XCTAssertEqual(valueFromStream, 3209099, "Longs don't match.")

    }

    func testFloatValue() {
        let avroBytes: [UInt8] = [0xc3, 0xf5, 0x48, 0x40]
        let jsonSchema = "{ \"type\" : \"float\" }"
        let schema = Schema(jsonSchema)

        let expected: Float = 3.14
        let value = AvroValue(schema: schema, withBytes: avroBytes).float
        XCTAssertEqual(value, expected, "Floats don't match.")
            
        let inputStream = InputStream(data: Data(bytes: avroBytes))
        inputStream.open()
        let valueFromStream = AvroValue(schema: schema, withInputStream: inputStream).float
        XCTAssertEqual(valueFromStream, expected, "Floats don't match.")

    }

    func testDoubleValue() {
        let avroBytes: [UInt8] = [0x1f, 0x85, 0xeb, 0x51, 0xb8, 0x1e, 0x9, 0x40]
        let jsonSchema = "{ \"type\" : \"double\" }"
        let schema = Schema(jsonSchema)

        let expected: Double = 3.14
        let value = AvroValue(schema: schema, withBytes: avroBytes).double
        XCTAssertEqual(value, expected, "Doubles don't match.")
        
        let inputStream = InputStream(data: Data(bytes: avroBytes))
        inputStream.open()
        let valueFromStream = AvroValue(schema: schema, withInputStream: inputStream).double
        XCTAssertEqual(valueFromStream, expected, "Doubles don't match.")
    }

    func testBooleanValue() {
        let avroFalseBytes: [UInt8] = [0x0]
        let avroTrueBytes: [UInt8] = [0x1]

        let jsonSchema = "{ \"type\" : \"boolean\" }"
        let schema = Schema(jsonSchema)

        let valueTrue = AvroValue(schema: schema, withBytes: avroTrueBytes).boolean
        XCTAssertEqual(valueTrue, true, "Value should be true.")
        
        let trueInputStream = InputStream(data: Data(bytes: avroTrueBytes))
        trueInputStream.open()
        let valueTrueFromStream = AvroValue(schema: schema, withInputStream: trueInputStream).boolean
        XCTAssertEqual(valueTrueFromStream, true, "Value should be true.")

        let valueFalse = AvroValue(schema: schema, withBytes: avroFalseBytes).boolean
        XCTAssertEqual(valueFalse, false, "Value should be false.")
            
        let falseInputStream = InputStream(data: Data(bytes: avroFalseBytes))
        falseInputStream.open()
        let valueFalseFromStream = AvroValue(schema: schema, withInputStream: falseInputStream).boolean
        XCTAssertEqual(valueFalseFromStream, false, "Value should be false.")
    }

    func longFrom(_ avroValue: AvroValue) -> Int64? {
        let value = avroValue.long
        if value == nil {
            XCTFail("All values in array should be longs")
        }
        return value
    }
    
    func testArrayValue() {
        let avroBytes: [UInt8] = [0x04, 0x06, 0x36, 0x00]
        let expected: [Int64] = [3, 27]
        let jsonSchema = "{ \"type\" : \"array\", \"items\" : \"long\" }"
        let schema = Schema(jsonSchema)

        let avroValues = AvroValue(schema: schema, withBytes: avroBytes).array
        if let values = avroValues?.flatMap({ longFrom($0) }) {
            XCTAssertEqual(values, expected, "Arrays don't match.")
        } else {
            XCTFail("Nil value")
        }
        
        let inputStream = InputStream(data: Data(bytes: avroBytes))
        inputStream.open()
        
        let avroValuesFromStream = AvroValue(schema: schema, withInputStream: inputStream).array
        if let values = avroValuesFromStream?.flatMap({ longFrom($0) }) {
            XCTAssertEqual(values, expected, "Arrays don't match.")
        } else {
            XCTFail("Nil value")
        }
    }

    func longMapFrom(_ avroValue: AvroValue) -> Dictionary<String, Int64> {
        var result = Dictionary<String, Int64>()
        
        if let map = avroValue.map {
            for (key, avroValue) in map {
                if let long = avroValue.long {
                    result[key] = long
                }
            }
        }
        return result
    }
    
    func testMapValue() {
        let avroBytes: [UInt8] = [0x02, 0x06, 0x66, 0x6f, 0x6f, 0x36, 0x00]
        let expected = ["foo" : Int64(27)]
        let jsonSchema = "{ \"type\" : \"map\", \"values\" : \"long\" }"
        let schema = Schema(jsonSchema)

        let map = longMapFrom(AvroValue(schema: schema, withBytes: avroBytes))
        XCTAssertEqual(map, expected, "Dictionaries don't match.")

        let inputStream = InputStream(data: Data(bytes: avroBytes))
        inputStream.open()
        let mapFromStream = longMapFrom(AvroValue(schema: schema, withInputStream: inputStream))
        XCTAssertEqual(mapFromStream, expected, "Dictionaries don't match.")
    }

    func testUnionValue() {
        let avroBytes: [UInt8] = [0x02, 0x02, 0x61]
        let jsonSchema = "{\"type\" : [\"null\",\"string\"] }"
        let schema = Schema(jsonSchema)

        let value = AvroValue(schema: schema, withBytes: avroBytes).string
        XCTAssertEqual(value, "a", "Unexpected string value.")
        
        let inputStream = InputStream(data: Data(bytes: avroBytes))
        inputStream.open()
        let valueFromStream = AvroValue(schema: schema, withInputStream: inputStream).string
        XCTAssertEqual(valueFromStream, "a", "Unexpected string value.")
    }
    
    func testEnumValue() {
        let jsonSchema = "{\"type\" : \"enum\", \"name\" : \"myEnumType\", \"symbols\" : [\"foo\",\"bar\"] }"
        let schema = Schema(jsonSchema)

        let avroBytes1: [UInt8] = [0x00]
        let value1 = AvroValue(schema: schema, withBytes: avroBytes1)
        XCTAssertEqual(value1.enumeration, "foo")
        XCTAssertEqual(value1.enumerationRawValue, 0)
        let inputStream1 = InputStream(data: Data(bytes: avroBytes1))
        inputStream1.open()
        let valueFromStream1 = AvroValue(schema: schema, withInputStream: inputStream1)
        XCTAssertEqual(valueFromStream1.enumeration, "foo")
        XCTAssertEqual(valueFromStream1.enumerationRawValue, 0)

        let avroBytes2: [UInt8] = [0x02]
        let value2 = AvroValue(schema: schema, withBytes: avroBytes2)
        XCTAssertEqual(value2.enumeration, "bar")
        XCTAssertEqual(value2.enumerationRawValue, 1)
        let inputStream2 = InputStream(data: Data(bytes: avroBytes2))
        inputStream2.open()
        let valueFromStream2 = AvroValue(schema: schema, withInputStream: inputStream2)
        XCTAssertEqual(valueFromStream2.enumeration, "bar")
        XCTAssertEqual(valueFromStream2.enumerationRawValue, 1)

        let avroBytes3: [UInt8] = [0x01]
        let value3 = AvroValue(schema: schema, withBytes: avroBytes3)
        XCTAssertNil(value3.enumeration)
        XCTAssertNil(value3.enumerationRawValue)
        let inputStream3 = InputStream(data: Data(bytes: avroBytes3))
        inputStream3.open()
        let valueFromStream3 = AvroValue(schema: schema, withInputStream: inputStream3)
        XCTAssertNil(valueFromStream3.enumeration)
        XCTAssertNil(valueFromStream3.enumerationRawValue)

        let avroBytes4: [UInt8] = [0x04]
        let value4 = AvroValue(schema: schema, withBytes: avroBytes4)
        XCTAssertNil(value4.enumeration)
        XCTAssertNil(value4.enumerationRawValue)
        let inputStream4 = InputStream(data: Data(bytes: avroBytes4))
        inputStream4.open()
        let valueFromStream4 = AvroValue(schema: schema, withInputStream: inputStream4)
        XCTAssertNil(valueFromStream4.enumeration)
        XCTAssertNil(valueFromStream4.enumerationRawValue)
    }
}
