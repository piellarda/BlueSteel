//
//  AvroFileReader.swift
//  BlueSteel
//
//  Created by Jean-Alexis Montignies on 22.12.16.
//
//

import Foundation

class AvroFileReader {
    var schema: Schema!
    var readSchema: Schema?
    var url: URL?
    var data: Data?
    var inputStream: InputStream?
    var sync: [UInt8]?
    var remainingObjectCountInBlock = 0
    var blockBytesCount = 0
    
    enum State {
        case idle, unrecoverableError, opened, inBlock, closed
    }
    
    var state = State.idle
    
    public init(schema: Schema?, url: URL) {
        self.schema = schema
        self.url = url
    }

    public init(schema: Schema?, data: Data) {
        self.schema = schema
        self.data = data
    }

    func openFileAndParseHeaders() throws {
        do {
            if let data = data {
                inputStream = InputStream(data: data)
            }
            if let url = url {
                inputStream = InputStream(url: url)
            }
            
            guard inputStream != nil else {
                throw AvroError.errorOpeningFileForReading
            }
            inputStream!.open()
            
            let header = AvroValue(schema: AvroFileContainer.avroFileContainerSchema, withInputStream: inputStream!)
            switch header {
            case let .avroRecordValue(headerValues):
                guard let magicBytes = headerValues[AvroFileContainer.headerMagicKey], let bytes = magicBytes.fixed, bytes == AvroFileContainer.magic else {
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
                
                if schema == nil {
                    schema = readSchema
                }
                
                guard let syncValue = headerValues[AvroFileContainer.headerSyncKey], let sync = syncValue.fixed else {
                    throw AvroError.errorReadingFileSync
                }
                self.sync = sync
            default:
                throw AvroError.errorNotAnAvroFile
            }
            
        } catch let localError {
            throw localError
        }
    }
    
    func read() throws -> AvroValue? {
        if state == .idle {
            do {
                try openFileAndParseHeaders()
                state = .opened
            } catch let error {
                state = .unrecoverableError
                throw error
            }
        }
        
        if state == .opened {
            let decoder = AvroDecoder(inputStream!)
            if let readLong = decoder.decodeLong() {
                remainingObjectCountInBlock = Int(readLong)
            } else {
                inputStream!.close()
                state = .closed
                return nil
            }
            
            if let readLong = decoder.decodeLong() {
                blockBytesCount = Int(readLong)
            } else {
                state = .unrecoverableError
                throw AvroError.errorReadingBlockHeader
            }
            state = .inBlock
        }
        
        if state == .inBlock {
            let value = AvroValue(schema: schema, withInputStream: inputStream!)
            switch value {
            case .avroInvalidValue:
                state = .unrecoverableError
                throw AvroError.errorReadingObject
            default:
                remainingObjectCountInBlock -= 1
                if remainingObjectCountInBlock == 0 {
                    let decoder = AvroDecoder(inputStream!)
                    if let readSync = decoder.decodeFixed(16), readSync == sync! {
                        state = .opened
                        return value
                    } else {
                        state = .unrecoverableError
                        throw AvroError.errorReadingFileSync
                    }
                } else {
                    return value
                }
            }
        }
        
        return nil
    }
}
