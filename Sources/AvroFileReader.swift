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
    var readSchema: Schema?
    var url: URL
    var inputStream: InputStream?
    var sync: [UInt8]?
    
    public init(schema: Schema, URL: URL) {
        self.schema = schema
        self.url = URL
    }

    func openFileAndParseHeaders() throws {
        do {
            inputStream = InputStream(url: url)
            guard inputStream != nil else {
                throw AvroError.errorOpeningFileForReading
            }
            inputStream!.open()
            
            let header = AvroValue(schema: AvroFileContainer.avroFileContainerSchema, withInputStream: inputStream!)
            if let headerValues = header.map {
                guard let magicBytes = headerValues[AvroFileContainer.headerMagicKey], let bytes = magicBytes.bytes, bytes == AvroFileContainer.magic else {
                    throw AvroError.errorNotAnAvroFile
                }
                
                guard let metaData = headerValues[AvroFileContainer.headerMetaDataKey], let meta = metaData.map else {
                    throw AvroError.errorReadingFileSchema
                }
                
                guard let schemaValue = meta[AvroFileContainer.metaDataSchemaKey], let schemaString = schemaValue.string else {
                    throw AvroError.errorReadingFileSchema
                }
                
                readSchema = Schema(schemaString)
                
                guard readSchema != nil else {
                    throw AvroError.errorReadingFileSchema
                }
                
                guard let syncValue = headerValues[AvroFileContainer.headerSyncKey], let sync = syncValue.bytes else {
                    throw AvroError.errorReadingFileSync
                }
                self.sync = sync
                
            } else {
                throw AvroError.errorNotAnAvroFile
            }
            
        } catch let localError {
            throw localError
        }
    }
    
    
}
