//
//  AvroEncoder.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

open class AvroEncoder {
    convenience init(capacity: Int) {
        self.init()
        bytes.reserveCapacity(capacity)
    }
    
    var bytes: [UInt8] = []

    func encodeNull() {
        return
    }

    func encodeBoolean(_ value: Bool) {
        if value {
            bytes.append(UInt8(0x1))
        } else {
            bytes.append(UInt8(0x0))
        }
        return
    }

    func encodeInt(_ value: Int32) {
        let encoded = Varint(fromValue: Int64(value).encodeZigZag())
        bytes += encoded.backing
        return
    }

    func encodeLong(_ value: Int64) {
        let encoded = Varint(fromValue: value.encodeZigZag())
        bytes += encoded.backing
        return
    }
    
    func encodeFloat(_ value: Float) {
        let bits: UInt32 = unsafeBitCast(value, to: UInt32.self)

        let encodedFloat = [UInt8(0xff & bits),
            UInt8(0xff & (bits >> 8)),
            UInt8(0xff & (bits >> 16)),
            UInt8(0xff & (bits >> 24))]

        bytes += encodedFloat
        return
    }
    
    func encodeDouble(_ value: Double) {
        let bits: UInt64 = unsafeBitCast(value, to: UInt64.self)

        let encodedDouble = [UInt8(0xff & bits),
            UInt8(0xff & (bits >> 8)),
            UInt8(0xff & (bits >> 16)),
            UInt8(0xff & (bits >> 24)),
            UInt8(0xff & (bits >> 32)),
            UInt8(0xff & (bits >> 40)),
            UInt8(0xff & (bits >> 48)),
            UInt8(0xff & (bits >> 56))]
        bytes += encodedDouble
        return
    }

    func encodeString(_ value: String) {
        encodeBytes([UInt8](value.utf8))
    }

    func encodeBytes(_ value: [UInt8]) {
        encodeLong(Int64(value.count))
        bytes += value
        return
    }

    func encodeFixed(_ value: [UInt8]) {
        bytes += value
        return
    }

    public init() {
        bytes = []
    }
}
