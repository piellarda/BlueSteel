//
//  AvroSchemaTests.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import XCTest
import BlueSteel

class AvroSchemaTests: XCTestCase {

    override func setUp() {
        super.setUp()
    }

    override func tearDown() {
        super.tearDown()
    }

    func testSchemaEquality(_ s1: String, s2: String) {
        let lhs = Schema(s1)
        let rhsEqual = Schema(s1)
        let rhsNotEqual = Schema(s2)

        XCTAssertEqual(lhs, rhsEqual, "Schemas should be equal")
        XCTAssertNotEqual(lhs, rhsNotEqual, "Schemas should not be equal")
    }

    func testPrimitive() {
        let jsonSchema = "{ \"type\" : \"long\"}"
        let schema = Schema(jsonSchema)
        XCTAssertNotNil(schema)

        switch schema! {
        case .avroLongSchema :
            XCTAssert(true, "Passed.")
        default:
            XCTAssert(false, "Failed.")
        }
    }

    func testMap() {
        let jsonSchema = "{ \"type\" : \"map\", \"values\" : \"int\" }"
        let schema = Schema(jsonSchema)
        XCTAssertNotNil(schema)

        switch schema! {
        case .avroMapSchema(let box):
            switch box.value {
            case .avroIntSchema:
                XCTAssert(true, "Passed.")
            default:
                XCTAssert(false, "Failed: Map of wrong type.")
            }
        default:
            XCTAssert(false, "Failed.")
        }
    }

    func testMapEquality() {
        let lJsonSchema = "{ \"type\" : \"map\", \"values\" : \"bytes\" }"
        let rJsonSchema = "{ \"type\" : \"map\", \"values\" : \"string\" }"

        self.testSchemaEquality(lJsonSchema, s2: rJsonSchema)
    }

    func testArray() {
        let jsonSchema = "{ \"type\" : \"array\", \"items\" : \"double\" }"
        let schema = Schema(jsonSchema)
        XCTAssertNotNil(schema)

        switch schema! {
        case .avroArraySchema(let box):
            switch box.value {
            case .avroDoubleSchema:
                XCTAssert(true, "Passed.")
            default:
                XCTAssert(false, "Failed: Map of wrong type.")
            }
        default:
            XCTAssert(false, "Failed.")
        }
    }

    func testArrayEquality() {
        let lJsonSchema = "{ \"type\" : \"array\", \"items\" : { \"type\" : \"map\", \"values\" : \"int\" } }"
        let rJsonSchema = "{ \"type\" : \"array\", \"items\" : { \"type\" : \"map\", \"values\" : \"long\" } }"

        self.testSchemaEquality(lJsonSchema, s2: rJsonSchema)
    }

    func testArrayMap() {
        let jsonSchema = "{ \"type\" : \"array\", \"items\" : { \"type\" : \"map\", \"values\" : \"int\" } }"
        let schema = Schema(jsonSchema)
        XCTAssertNotNil(schema)

        switch schema! {
        case .avroArraySchema(let arrayBox) :
            switch arrayBox.value {
            case .avroMapSchema(let mapBox) :
                switch mapBox.value {
                case .avroIntSchema :
                    XCTAssert(true, "Passed.")
                default:
                    XCTAssert(false, "Failed: Map of wrong type.")
                }
            default :
                XCTAssert(false, "Failed: Array of wrong type.")
            }
        default:
            XCTAssert(false, "Failed.")
        }
    }

    func testUnion() {
        let jsonSchema = "{ \"type\" : [ \"double\", \"int\", \"long\", \"float\" ] }"
        let expected: [Schema] = [.avroDoubleSchema, .avroIntSchema, .avroLongSchema, .avroFloatSchema]
        let schema = Schema(jsonSchema)
        XCTAssertNotNil(schema)

        switch schema! {
        case .avroUnionSchema(let schemas):
            XCTAssert(schemas.count == 4, "Wrong number of schemas in union.")
            for idx in 0...3 {
                switch schemas[idx] {
                case let res where res == expected[idx] :
                    XCTAssert(true, "Passed")
                default :
                    XCTAssert(false, "Wrong schema type in union.")
                }
            }
        default:
            XCTAssert(false, "Failed.")
        }
    }

    func testUnionEquality() {
        let lJsonSchema = "{ \"type\" : [ \"double\", \"int\", \"long\", \"float\" ] }"
        let rJsonSchema = "{ \"type\" : [ \"double\", \"float\", \"int\", \"long\" ] }"

        self.testSchemaEquality(lJsonSchema, s2: rJsonSchema)
    }

    func testUnionMap() {
        let jsonSchema = "{ \"type\" : [ { \"type\" : \"map\", \"values\" : \"int\" }, { \"type\" : \"map\", \"values\" : \"double\" } ] }"
        let expected: [Schema] = [.avroIntSchema, .avroDoubleSchema]
        let schema = Schema(jsonSchema)
        XCTAssertNotNil(schema)

        switch schema! {
        case .avroUnionSchema(let schemas):
            XCTAssert(schemas.count == 2, "Wrong number of schemas in union.")
            for idx in 0...1 {
                switch schemas[idx] {
                case .avroMapSchema(let box) :
                    switch box.value {
                    case let res where res == expected[idx] :
                        XCTAssert(true, "Passed")
                    default :
                        XCTAssert(false, "Expected primitive schema type in map.")
                    }
                default :
                    XCTAssert(false, "Expected map schema type in union.")
                }
            }
        default:
            XCTAssert(false, "Failed.")
        }
    }

    func testRecord() {
        let jsonSchema = "{ \"type\" : \"record\", \"name\" : \"AddToCartActionEvent\", " +
            "\"doc\" : \"This event is fired when a user taps on the add to cart button.\"," +
            "\"fields\" : [ { \"name\" : \"lookId\", \"type\" : \"long\" }," +
            "{ \"name\" : \"productId\", \"type\" : \"long\" }," +
            "{ \"name\" : \"quantity\", \"type\" : \"int\" }," +
            "{ \"name\" : \"saleId\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }," +
        "{ \"name\" : \"skuId\",\"type\" : \"long\" }]}"

        let fieldNames = ["lookId", "productId", "quantity", "saleId", "skuId"]
        let fieldType: [Schema] = [.avroLongSchema, .avroLongSchema, .avroIntSchema, .avroInvalidSchema, .avroLongSchema]
        let unionFieldTypes: [Schema] = [.avroNullSchema, .avroLongSchema]
        let schema = Schema(jsonSchema)
        XCTAssertNotNil(schema)

        switch schema! {
        case .avroRecordSchema("AddToCartActionEvent", let fields) :
            XCTAssert(fields.count == 5, "Record schema should consist of 5 fields.")
            for idx in 0...4 {
                switch fields[idx] {
                case .avroFieldSchema(fieldNames[idx], let typeSchema) :
                    switch typeSchema.value {
                    case let res where res == fieldType[idx] :
                        XCTAssert(true, "")
                    case .avroUnionSchema(let unionSchemas) :
                        XCTAssert(unionSchemas.count == 2, "Union schema should consist of 2 fields.")
                        for uidx in 0...1 {
                            switch unionSchemas[uidx] {
                            case let res where res == unionFieldTypes[uidx] :
                                XCTAssert(true, "")
                            default :
                                XCTAssert(false, "Wrong type in union")
                            }
                        }
                    default :
                        XCTAssert(false, "Wrong field type.")
                    }
                    XCTAssert(true, "")
                default :
                    XCTAssert(false, "Failed.")
                }
            }
        default:
            XCTAssert(false, "Failed.")
        }
    }

    func testRecordEquality() {
        let lJsonSchema = "{ \"type\" : \"record\", \"name\" : \"AddToCartActionEvent\", " +
            "\"doc\" : \"This event is fired when a user taps on the add to cart button.\"," +
            "\"fields\" : [ { \"name\" : \"lookId\", \"type\" : \"long\" }," +
            "{ \"name\" : \"productId\", \"type\" : \"long\" }," +
            "{ \"name\" : \"quantity\", \"type\" : \"int\" }," +
            "{ \"name\" : \"saleId\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }," +
        "{ \"name\" : \"skuId\",\"type\" : \"long\" }]}"
        let rJsonSchema = "{ \"type\" : \"record\", \"name\" : \"AddToCartActionEvent\", " +
            "\"doc\" : \"This event is fired when a user taps on the add to cart button.\"," +
            "\"fields\" : [ { \"name\" : \"lookId\", \"type\" : \"long\" }," +
            "{ \"name\" : \"productId\", \"type\" : \"long\" }," +
            "{ \"name\" : \"quantity\", \"type\" : \"int\" }," +
            "{ \"name\" : \"saleId\", \"type\" : [ \"null\", \"float\" ], \"default\" : null }," +
        "{ \"name\" : \"skuId\",\"type\" : \"long\" }]}"

        self.testSchemaEquality(lJsonSchema, s2: rJsonSchema)
    }

    func testEnum() {
        let jsonSchema = "{ \"type\" : \"enum\", \"name\" : \"ChannelKey\", \"doc\" : \"Enum of valid channel keys.\", \"symbols\" : [ \"CityIphone\", \"CityMobileWeb\", \"GiltAndroid\", \"GiltcityCom\", \"GiltCom\", \"GiltIpad\", \"GiltIpadSafari\", \"GiltIphone\", \"GiltMobileWeb\", \"NoChannel\" ]}"

        let expectedSymbols = ["CityIphone", "CityMobileWeb", "GiltAndroid", "GiltcityCom", "GiltCom", "GiltIpad", "GiltIpadSafari", "GiltIphone", "GiltMobileWeb", "NoChannel"]
        let schema = Schema(jsonSchema)
        XCTAssertNotNil(schema)

        switch schema! {
        case .avroEnumSchema(let enumName, let symbols) :
            XCTAssertEqual(enumName, "ChannelKey", "Unexpected enum name.")
            XCTAssertEqual(symbols, expectedSymbols, "Symbols dont match.")
        default :
            XCTAssert(false, "Failed")
        }
    }

    func testEnumEquality() {
        // Name Checks
        let lnJsonSchema = "{ \"type\" : \"enum\", \"name\" : \"ChannelKey\", \"doc\" : \"Enum of valid channel keys.\", \"symbols\" : [ \"CityIphone\", \"CityMobileWeb\", \"GiltAndroid\", \"GiltcityCom\", \"GiltCom\", \"GiltIpad\", \"GiltIpadSafari\", \"GiltIphone\", \"GiltMobileWeb\", \"NoChannel\" ]}"
        let rnJsonSchema = "{ \"type\" : \"enum\", \"name\" : \"ChanelKey\", \"doc\" : \"Enum of valid channel keys.\", \"symbols\" : [ \"CityIphone\", \"CityMobileWeb\", \"GiltAndroid\", \"GiltcityCom\", \"GiltCom\", \"GiltIpad\", \"GiltIpadSafari\", \"GiltIphone\", \"GiltMobileWeb\", \"NoChannel\" ]}"

        self.testSchemaEquality(lnJsonSchema, s2: rnJsonSchema)

        let lvJsonSchema = "{ \"type\" : \"enum\", \"name\" : \"ChannelKey\", \"doc\" : \"Enum of valid channel keys.\", \"symbols\" : [ \"CityIphone\", \"CityMobileWeb\", \"GiltAndroid\", \"GiltcityCom\", \"GiltCom\", \"GiltIpad\", \"GiltIpadSafari\", \"GiltIphone\", \"GiltMobileWeb\", \"NoChannel\" ]}"
        let rvJsonSchema = "{ \"type\" : \"enum\", \"name\" : \"ChannelKey\", \"doc\" : \"Enum of valid channel keys.\", \"symbols\" : [ \"CityIphone\", \"CityMobileWeb\", \"GiltAndroid\", \"GiltcityCom\", \"GilCom\", \"GiltIpad\", \"GiltIpadSafari\", \"GiltIphone\", \"GiltMobileWeb\", \"NoChannel\" ]}"

        self.testSchemaEquality(lvJsonSchema, s2: rvJsonSchema)
    }

    func testFixed() {
        let jsonSchema = "{ \"type\" : \"fixed\", \"name\" : \"Uuid\", \"size\" : 16 }"
        let schema = Schema(jsonSchema)
        XCTAssertNotNil(schema)

        switch schema! {
        case .avroFixedSchema(let fixedName, let size) :
            XCTAssertEqual("Uuid", fixedName, "Unexpected fixed name.")
            XCTAssertEqual(16, size, "Unexpected fixed size.")
        default :
            XCTAssert(false, "Failed.")
        }
    }

    func testFixedEquality() {
        let lnJsonSchema = "{ \"type\" : \"fixed\", \"name\" : \"Uuid\", \"size\" : 16 }"
        let rnJsonSchema = "{ \"type\" : \"fixed\", \"name\" : \"id\", \"size\" : 16 }"

        self.testSchemaEquality(lnJsonSchema, s2: rnJsonSchema)

        let lvJsonSchema = "{ \"type\" : \"fixed\", \"name\" : \"Uuid\", \"size\" : 16 }"
        let rvJsonSchema = "{ \"type\" : \"fixed\", \"name\" : \"Uuid\", \"size\" : 10 }"

        self.testSchemaEquality(lvJsonSchema, s2: rvJsonSchema)
    }

    func testFingerprint() {
        //let jsonSchema = "{ \"type\" : \"enum\", \"name\" : \"ChannelKey\", \"doc\" : \"Enum of valid channel keys.\", \"symbols\" : [ \"CityIphone\", \"CityMobileWeb\", \"GiltAndroid\", \"GiltcityCom\", \"GiltCom\", \"GiltIpad\", \"GiltIpadSafari\", \"GiltIphone\", \"GiltMobileWeb\", \"NoChannel\" ]}"
        let jsonSchema = "{\"type\":\"record\",\"name\":\"StorePageViewedEvent\",\"namespace\":\"com.gilt.mobile.tapstream.v1\",\"doc\":\"This event is fired when a store is displayed.\",\"fields\":[{\"name\":\"uuid\",\"type\":{\"type\":\"fixed\",\"name\":\"UUID\",\"namespace\":\"gfc.avro\",\"size\":16},\"doc\":\"the unique identifier of the event, as determined by the mobile app.\\n        this must be a version 1 uuid.\"},{\"name\":\"base\",\"type\":{\"type\":\"record\",\"name\":\"MobileEvent\",\"doc\":\"Fields common to all events generated by mobile apps.\\n      NOTE: this should not be sent as is, meant to be wrapped into some more specific type.\",\"fields\":[{\"name\":\"eventTs\",\"type\":\"long\",\"doc\":\"The unix timestamp at which the event occurred.\\n        This is in Gilt time (not device time).\"},{\"name\":\"batchGuid\",\"type\":\"gfc.avro.UUID\",\"doc\":\"NOTE: This attribute should NOT be set by the client, it will be set by the server.\\n        The unique identifier assigned to a batch of events.\\n        Events that share this value were submitted by a client as part of the same batch.\",\"default\":\"\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\"},{\"name\":\"channelKey\",\"type\":{\"type\":\"enum\",\"name\":\"ChannelKey\",\"doc\":\"Enum of valid channel keys.\",\"symbols\":[\"CityIphone\",\"CityMobileWeb\",\"GiltAndroid\",\"GiltcityCom\",\"GiltCom\",\"GiltIpad\",\"GiltIpadSafari\",\"GiltIphone\",\"GiltMobileWeb\",\"NoChannel\"]}},{\"name\":\"deviceTimeOffset\",\"type\":\"long\",\"doc\":\"Offset in milliseconds between the Gilt time and the device time (device time + offset == Gilt time)\"},{\"name\":\"headers\",\"type\":{\"type\":\"map\",\"values\":\"string\"},\"doc\":\"The HTTP headers of the request the event was sent in.\\n        Multi-valued header values are tab-separated.\\n        NOTE: This attribute should NOT be set by the client, it will be set by the server.\",\"default\":{}},{\"name\":\"ipAddress\",\"type\":\"string\",\"doc\":\"IP address of the client.\\n        NOTE: This attribute should NOT be set by the client, it will be set by the server.\",\"default\":\"0.0.0.0\"},{\"name\":\"sessionTs\",\"type\":\"long\",\"doc\":\"The unix timestamp of the current session.\"},{\"name\":\"testBucketId\",\"type\":\"long\",\"doc\":\"The test bucket identifier.\"},{\"name\":\"userAgent\",\"type\":\"string\",\"doc\":\"The user agent of the request.\\n        NOTE: This attribute should NOT be set by the client, it will be set by the server.\",\"default\":\"\"},{\"name\":\"userGuid\",\"type\":[\"null\",\"gfc.avro.UUID\"],\"doc\":\"The Gilt user_guid (optional).\",\"default\":null},{\"name\":\"visitorGuid\",\"type\":\"gfc.avro.UUID\",\"doc\":\"Generated on first app launch it never changes unless the app is uninstalled and re-installed.\"}]}},{\"name\":\"page\",\"type\":{\"type\":\"record\",\"name\":\"PageViewedEvent\",\"doc\":\"Fields common to all events of type page_viewed.\\n      NOTE: this should not be sent as is, meant to be wrapped into some more specific type.\",\"fields\":[{\"name\":\"deviceOrientation\",\"type\":{\"type\":\"enum\",\"name\":\"DeviceOrientation\",\"doc\":\"Enum of valid device orientations.\",\"symbols\":[\"Landscape\",\"Portrait\"]}}]}},{\"name\":\"storeKey\",\"type\":{\"type\":\"enum\",\"name\":\"StoreKey\",\"doc\":\"Enum of valid store keys.\",\"symbols\":[\"Children\",\"City\",\"Gifts\",\"Home\",\"Men\",\"MyGilt\",\"Women\",\"NoStore\"]}}]}"

        let schema = Schema(jsonSchema)
        XCTAssertNotNil(schema)

        var existingTypes:[String] = []
        schema!.parsingCanonicalForm(&existingTypes)

        if let fp = schema!.fingerprint() {
            var hexString = ""
            for byte in fp {
                hexString += NSString(format: "%02X", byte) as String
            }
            XCTAssertEqual(hexString, "1E85E88ACE91D3273377213306CDFAF2F0FE705F93E95BF4F8065BC10F1D55FE", "Fingeprint mismatch.")
        } else {
            XCTFail("Nil fingerprint.")
        }
    }
}
