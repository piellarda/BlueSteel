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

        let correctAvroValue = AvroValue.AvroEnumValue(1, "CityMobileWeb")
        let extraAvroValue = AvroValue.AvroEnumValue(10, "ExtraChannel")
        let typoAvroValue = AvroValue.AvroEnumValue(5, "GiltiPad")
        XCTAssertEqual(correctAvroValue.encode(schema)!, [0x2])
        XCTAssertNil(extraAvroValue.encode(schema), "")
        XCTAssertNil(typoAvroValue.encode(schema), "")
    }
}
