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

        if let value = AvroValue(jsonSchema: jsonSchema, withBytes: avroBytes)?.string {
            XCTAssertEqual(value, "foo", "Strings don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }

    func testByteValue() {
        let avroBytes: [UInt8] = [0x06, 0x66, 0x6f, 0x6f]
        let jsonSchema = "{ \"type\" : \"bytes\" }"

        if let value = AvroValue(jsonSchema: jsonSchema, withBytes: avroBytes)?.bytes {
            XCTAssertEqual(value, [0x66, 0x6f, 0x6f], "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }

    func testIntValue() {
        let avroBytes: [UInt8] = [0x96, 0xde, 0x87, 0x3]
        let jsonSchema = "{ \"type\" : \"int\" }"

        if let value = AvroValue(jsonSchema: jsonSchema, withBytes: avroBytes)?.integer {
            XCTAssertEqual(Int(value), 3209099, "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }

    func testLongValue() {
        let avroBytes: [UInt8] = [0x96, 0xde, 0x87, 0x3]
        let jsonSchema = "{ \"type\" : \"long\" }"

        if let value = AvroValue(jsonSchema: jsonSchema, withBytes: avroBytes)?.long {
            XCTAssertEqual(Int(value), 3209099, "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }

    func testFloatValue() {
        let avroBytes: [UInt8] = [0xc3, 0xf5, 0x48, 0x40]
        let jsonSchema = "{ \"type\" : \"float\" }"

        let expected: Float = 3.14
        if let value = AvroValue(jsonSchema: jsonSchema, withBytes: avroBytes)?.float {
            XCTAssertEqual(value, expected, "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }

    func testDoubleValue() {
        let avroBytes: [UInt8] = [0x1f, 0x85, 0xeb, 0x51, 0xb8, 0x1e, 0x9, 0x40]
        let jsonSchema = "{ \"type\" : \"double\" }"

        let expected: Double = 3.14
        if let value = AvroValue(jsonSchema: jsonSchema, withBytes: avroBytes)?.double {
            XCTAssertEqual(value, expected, "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }

    func testBooleanValue() {
        let avroFalseBytes: [UInt8] = [0x0]
        let avroTrueBytes: [UInt8] = [0x1]

        let jsonSchema = "{ \"type\" : \"boolean\" }"

        if let value = AvroValue(jsonSchema: jsonSchema, withBytes: avroTrueBytes)?.boolean {
            XCTAssert(value, "Value should be true.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }

        if let value = AvroValue(jsonSchema: jsonSchema, withBytes: avroFalseBytes)?.boolean {
            XCTAssert(!value, "Value should be false.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }

    func testArrayValue() {
        let avroBytes: [UInt8] = [0x04, 0x06, 0x36, 0x00]
        let expected: [Int64] = [3, 27]
        let jsonSchema = "{ \"type\" : \"array\", \"items\" : \"long\" }"

        if let values = AvroValue(jsonSchema: jsonSchema, withBytes: avroBytes)?.array {
            XCTAssertEqual(values.count, 2, "Wrong number of elements in array.")
            for idx in 0...1 {
                if let value = values[idx].long {
                    XCTAssertEqual(value, expected[idx], "Unexpected value.")
                } else {
                    XCTAssert(false, "Failed. Nil value")
                }
            }
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }

    func testMapValue() {
        let avroBytes: [UInt8] = [0x02, 0x06, 0x66, 0x6f, 0x6f, 0x36, 0x00]
        let expected: [Int64] = [27]
        let jsonSchema = "{ \"type\" : \"map\", \"values\" : \"long\" }"

        if let pairs = AvroValue(jsonSchema: jsonSchema, withBytes: avroBytes)?.map {
            XCTAssertEqual(pairs.count, 1, "Wrong number of elements in map.")

            if let value = pairs["foo"]?.long {
                    XCTAssertEqual(value, expected[0], "Unexpected value.")
            } else {
                XCTAssert(false, "Failed. Nil value")
            }

        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }

    func testEnumValue() {
        let avroBytes: [UInt8] = [0x12]
        let jsonSchema = "{ \"type\" : \"enum\", \"name\" : \"ChannelKey\", \"doc\" : \"Enum of valid channel keys.\", \"symbols\" : [ \"CityIphone\", \"CityMobileWeb\", \"GiltAndroid\", \"GiltcityCom\", \"GiltCom\", \"GiltIpad\", \"GiltIpadSafari\", \"GiltIphone\", \"GiltMobileWeb\", \"NoChannel\" ]}"

        let value = AvroValue(jsonSchema: jsonSchema, withBytes: avroBytes)
        XCTAssertNotNil(value)

        switch value! {
        case .avroEnumValue(let index, let string):
            XCTAssertEqual(index, 9)
            XCTAssertEqual(string, "NoChannel")
        case _:
            XCTAssert(false, "Invalid avro value")
        }
    }

    func testUnionValue() {
        let avroBytes: [UInt8] = [0x02, 0x02, 0x61]
        let jsonSchema = "{\"type\" : [\"null\",\"string\"] }"
        if let value = AvroValue(jsonSchema: jsonSchema, withBytes: avroBytes)?.string {
            XCTAssertEqual(value, "a", "Unexpected string value.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }

    }
}
