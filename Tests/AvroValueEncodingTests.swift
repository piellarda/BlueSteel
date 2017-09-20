//
//  AvroValueEncodingTests.swift
//  BlueSteel
//
//  Created by Nikita Korchagin on 29/09/16.
//  Copyright (c) 2016 Gilt. All rights reserved.
//

import XCTest
import BlueSteel

class AvroValueEncodingTests: XCTestCase {
    func testEnumEncoding() {
        let jsonSchema = "{ \"type\" : \"enum\", \"name\" : \"ChannelKey\", \"doc\" : \"Enum of valid channel keys.\", \"symbols\" : [ \"CityIphone\", \"CityMobileWeb\", \"GiltAndroid\", \"GiltcityCom\", \"GiltCom\", \"GiltIpad\", \"GiltIpadSafari\", \"GiltIphone\", \"GiltMobileWeb\", \"NoChannel\" ]}"
        let schema = Schema(jsonSchema)
        XCTAssertNotNil(schema)

        let correctAvroValue = AvroValue.avroEnumValue(1, "CityMobileWeb")
        let extraAvroValue = AvroValue.avroEnumValue(10, "ExtraChannel")
        let typoAvroValue = AvroValue.avroEnumValue(5, "GiltiPad")
        let encodedCorrectValue = correctAvroValue.encode(schema!)
        XCTAssertNotNil(encodedCorrectValue)
        XCTAssertEqual(encodedCorrectValue!, [0x2])
        XCTAssertNil(extraAvroValue.encode(schema!), "")
        XCTAssertNil(typoAvroValue.encode(schema!), "")
    }
}
