//
//  AvroFileReader.swift
//  BlueSteel
//
//  Created by Jean-Alexis Montignies on 22.12.16.
//
//

import Foundation

class AvroFileReader {
    var schema: Schema
    var url: URL
    var fileHandle: FileHandle?

    public init(schema: Schema, URL: URL) {
        self.schema = schema
        self.url = URL
    }

    func openFileAndParseHeaders() throws {
        do {
            fileHandle = try FileHandle(forReadingFrom: url)
        } catch let localError {
            throw localError
        }
    }
    
    
}
