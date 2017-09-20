//
//  VarintTests.swift
//  BlueSteelTests
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import XCTest
import BlueSteel

class VarintTests: XCTestCase {
    
    override func setUp() {
        super.setUp()
    }
    
    override func tearDown() {
        super.tearDown()
    }

    func testToInt() {
        //TODO: Exercise a range of numbers here.
        let expected = Int64(bitPattern: UInt64(arc4random()))
        let testvarint = Varint(fromValue: expected)
        let val = testvarint.toInt64()
        XCTAssertEqual(val, expected, "Expected -1. Got\(val)")
    }

    func testToUInt() {
        let expected = UInt64(arc4random())
        let testvarint = Varint(fromValue: expected)
        let val = testvarint.toUInt64()
        XCTAssertEqual(val, expected, "Expected -1. Got\(val)")
    }

    func testEncodeZigZag() {
        let val = Int64(Int32.max).encodeZigZag()
        XCTAssertEqual(val, UInt64(UInt32.max) - UInt64(1), "\(val)")
    }

    func testDecodeZigZag() {
        let val = UInt64.max.decodeZigZag()
        XCTAssertEqual(val, Int64.min, "\(val)")
    }
}
