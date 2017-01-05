//
//  AvroFileContainerTests.swift
//  BlueSteel
//
//  Created by Jean-Alexis Montignies on 05.01.17.
//
//

import XCTest
@testable import BlueSteel

class AvroFileContainerTests: XCTestCase {
    var schema : Schema!
    
    override func setUp() {
        schema = Schema.avroRecordSchema("myRecord", [Schema.avroFieldSchema("myInt", Box(Schema.avroLongSchema)), Schema.avroFieldSchema("myString", Box(Schema.avroStringSchema))])
        super.setUp()
    }
    
    override func tearDown() {
        super.tearDown()
    }
    
    func testFileContainer() {
        let writer = AvroFileWriter(schema: schema)
        let count = 1000
        writer.blockSize = 1000
        
        for i in 1...count {
            let value = AvroValue.avroRecordValue([
                "myInt": AvroValue.avroLongValue(Int64(i)),
                "myString": AvroValue.avroStringValue(String(i))
                ])
            do {
                try writer.append(value: value)
            } catch {
                XCTFail()
                break
            }
        }
        
        XCTAssertTrue(writer.tryToClose())
        
        let writtenData = writer.outputData
        XCTAssertNotNil(writtenData)
        
        if let readData = writtenData {
            var readCount = 0
            let reader = AvroFileReader(schema: schema, data: readData)
            
            do {
                while let value = try reader.read() {
                    readCount += 1
                    switch value {
                    case let .avroRecordValue(dictionary):
                        XCTAssertEqual(dictionary["myInt"]?.long, Int64(readCount))
                        XCTAssertEqual(dictionary["myString"]?.string, String(readCount))
                    default:
                        XCTFail("Expected record value")
                    }
                }
            } catch {
                XCTFail("Unexpected exception thrown while reading")
            }
        }
        
    }
}
