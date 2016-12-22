//
//  AvroDecoder.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

// TODO: Make this thread safe.

open class AvroDecoder {
    var dataBytes: [UInt8]?
    var fileHandle: FileHandle?

    public init(_ data:Data) {
        let dataPointer = (data as NSData).bytes.bindMemory(to: UInt8.self, capacity: data.count)
        let bufferPointer = UnsafeBufferPointer<UInt8>(start: dataPointer, count: data.count)
        dataBytes = [UInt8](bufferPointer)
    }

    public init(_ data:[UInt8]) {
        dataBytes = data
    }

    private func getBytes(_ count: Int) -> [UInt8]? {
        if let fileHandle = fileHandle {
            let data = fileHandle.readData(ofLength: count)
            if data.count == count {
                let dataPointer = (data as NSData).bytes.bindMemory(to: UInt8.self, capacity: data.count)
                let bufferPointer = UnsafeBufferPointer<UInt8>(start: dataPointer, count: data.count)
                return [UInt8](bufferPointer)
            }
        } else if dataBytes != nil {
            if dataBytes!.count >= count {
                let bytes = [UInt8](dataBytes!.prefix(count))
                dataBytes!.removeSubrange(0...count-1)
                return bytes
            }
        }
        return nil
    }
    

    open func decodeNull() {
        // Nulls aren't actually encoded.
        return
    }

    open func decodeBoolean() -> Bool? {
        guard let bytes = getBytes(1) else {
            return nil
        }

        let result: Bool = bytes[0] > 0
        return result
    }
    
    open func decodeDouble() -> Double? {
        guard let slice = getBytes(8) else {
            return nil
        }

        var bits: UInt64 = UInt64(slice[slice.startIndex])
            bits |= UInt64(slice[slice.startIndex + 1]) << 8
            bits |= UInt64(slice[slice.startIndex + 2]) << 16
            bits |= UInt64(slice[slice.startIndex + 3]) << 24
            bits |= UInt64(slice[slice.startIndex + 4]) << 32
            bits |= UInt64(slice[slice.startIndex + 5]) << 40
            bits |= UInt64(slice[slice.startIndex + 6]) << 48
            bits |= UInt64(slice[slice.startIndex + 7]) << 56

        let result = withUnsafePointer(to: &bits, { (ptr: UnsafePointer<UInt64>) -> Double in
            return ptr.withMemoryRebound(to: Double.self, capacity: 1) { memory in
                return memory.pointee
            }
        })
        return result
    }


    open func decodeFloat() -> Float? {
        guard let slice = getBytes(4) else {
            return nil
        }
        
        var bits: UInt32 = UInt32(slice[slice.startIndex])
            bits |= UInt32(slice[slice.startIndex + 1]) << 8
            bits |= UInt32(slice[slice.startIndex + 2]) << 16
            bits |= UInt32(slice[slice.startIndex + 3]) << 24

        let result = withUnsafePointer(to: &bits, { (ptr: UnsafePointer<UInt32>) -> Float in
            return ptr.withMemoryRebound(to: Float.self, capacity: 1) { return $0.pointee }
        })
        return result
    }

    private func getVarInt() -> Varint? {
        if let handle = fileHandle {
            return Varint.VarintFromHandle(handle)
        } else if let bytes = dataBytes {
            if let varint = Varint.VarintFromBytes(bytes) {
                dataBytes!.removeSubrange(0...varint.count - 1)
                return varint
            }
        }
        return nil
    }
    
    open func decodeInt() -> Int32? {
        if let x = getVarInt() {
            return Int32(x.toUInt64().decodeZigZag())
        }
        return nil
    }

    open func decodeLong() -> Int64? {
        if let x = getVarInt() {
            return Int64(x.toUInt64().decodeZigZag())
        }
        return nil
    }

    // Avro doesnt actually support Unsigned primitives. So We'll keep this internal.
    internal func decodeUInt() -> UInt {
        // Stub
        return 0
    }

    open func decodeBytes() -> [UInt8]? {
        if let sizeLong = decodeLong() {
            let size = Int(sizeLong)
            return getBytes(size)
        }
        return nil
    }

    open func decodeString() -> String? {
        if let rawString = decodeBytes() {
            //return String.stringWithdataBytes(rawString, encoding: NSUTF8StringEncoding)
            //let result: String? = NSString(dataBytes: rawString, length: rawString.count, encoding: NSUTF8StringEncoding)
            let result = String(bytes: rawString, encoding: String.Encoding.utf8)
            return result
        } else {
            return nil
        }
    }

    open func decodeFixed(_ size: Int) -> [UInt8]? {
        return getBytes(size)
    }
}
