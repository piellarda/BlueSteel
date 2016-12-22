//
//  AvroDecoderTests.swift
//  BlueSteelTests
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import XCTest
import BlueSteel

class AvroDecoderTests: XCTestCase {
    var Decoder: AvroDecoder? = nil

    override func setUp() {
        super.setUp()
        Decoder = AvroDecoder([0x4, 0x96, 0xde, 0x87, 0x3, 0xcd, 0xcc, 0x4c, 0x40, 0x96, 0xde, 0x87, 0x3])
    }

    override func tearDown() {
        super.tearDown()
    }

    func testDecodeInt() {
        let x = Decoder?.decodeInt()
        let y = Decoder?.decodeInt()
        XCTAssertEqual(x, 2, "Decode broken.")
        XCTAssertEqual(y, 3209099, "Decoder broken.")
    }

    func testDecodeLong() {
        XCTAssert(true, "Pass")
    }

    func testDecodeFloat() {
        XCTAssert(true, "Pass")
    }

    func testDecodeDouble() {
        XCTAssert(true, "Pass")
    }

    func testDecodeString() {
        XCTAssert(true, "Pass")
    }

}
