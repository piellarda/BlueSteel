//
//  AvroFileWriter.swift
//  BlueSteel
//
//  Created by Jean-Alexis Montignies on 07.10.16.
//
//

open class AvroFileWriter {
    

    var url: URL?
    var outputStream: OutputStream?
    var schema: Schema
    let sync: [UInt8] = AvroFileWriter.randomSync()
    var encoder : AvroEncoder?
    var estimatedEncodedObjectLength = 0
    public var blockSize = 100000
    var blockObjectCount = 0
    public private(set) var byteCount = 0
    public private(set) var objectCount = 0
    
    static func randomSync() -> [UInt8] {
        var sync : [UInt8] = []
        sync.reserveCapacity(16)
        for _ in 0...15 {
            sync.append(UInt8(arc4random_uniform(255)))
        }
        return sync
    }
    
    
    public init(schema: Schema, url: URL) {
        self.schema = schema
        self.url = url
    }
    
    public init(schema: Schema) {
        self.schema = schema
    }
    
    func openFileAndWriteHeader() throws {
        if let url = url, url.isFileURL {
            let fileManager = FileManager.default
            let path = url.path

            do {
                try fileManager.createDirectory(atPath: (path as NSString).deletingLastPathComponent, withIntermediateDirectories: true, attributes: nil)
            }
            catch  {
                throw AvroError.errorCreatingDirectory
            }
            if !fileManager.createFile(atPath: path, contents: nil, attributes: nil) {
                throw AvroError.errorCreatingFile
            }
        }
        
        guard let schemaJson = schema.json() else {
            throw AvroError.errorCreatingJSONSchema
        }
        
        let metaData = [
            AvroFileContainer.metaDataSchemaKey : AvroValue.avroStringValue(schemaJson)
        ]
        let headerFields = [
            AvroFileContainer.headerMagicKey : AvroValue.avroFixedValue(AvroFileContainer.magic),
            AvroFileContainer.headerMetaDataKey : AvroValue.avroMapValue(metaData),
            AvroFileContainer.headerSyncKey : AvroValue.avroFixedValue(sync)
        ]
        
        let header = AvroValue.avroRecordValue(headerFields)
        guard let avroFileSchema = AvroFileContainer.avroFileContainerSchema else {
            throw AvroError.errorReadingFileSchema
        }
        guard let encodedHeader = header.encode(avroFileSchema) else {
            throw AvroError.errorEncodingHeader
        }

        if let url = url {
            outputStream = OutputStream(url: url, append: false)
        } else {
            outputStream = OutputStream(toMemory: ())
        }
        
        guard outputStream != nil else {
            throw AvroError.errorCreatingFile
        }
        outputStream!.open()
        
        guard outputStream!.write(encodedHeader, maxLength: encodedHeader.count) == encodedHeader.count else {
            throw AvroError.errorWritting
        }
    }
    
    open func append(value: AvroValue) throws {
        if encoder == nil {
            encoder = AvroEncoder(capacity: blockSize)
        }
        
        encoder!.setCheckPoint()
        let previousByteCount = encoder!.checkPointByteCount
        
        guard let bytes = value.encode(encoder!, schema: schema) else {
            encoder?.revertToCheckPoint()
            throw AvroError.errorEncodingObject
        }
        
        blockObjectCount += 1
        objectCount += 1
        
        let objectEncodedLength = bytes.count - previousByteCount
        estimatedEncodedObjectLength = objectEncodedLength > estimatedEncodedObjectLength ? objectEncodedLength : estimatedEncodedObjectLength
        
        if bytes.count + estimatedEncodedObjectLength > blockSize {
            try closeBlock()
        }
    }
    
    func closeBlock() throws {
        guard encoder != nil else {
            return
        }

        guard blockObjectCount > 0  else {
            encoder = nil
            return
        }
        
        if outputStream == nil {
            try openFileAndWriteHeader()
        }
        

        let bytes = encoder!.bytes
        let blockEncoder = AvroEncoder(capacity: encoder!.bytes.count + 10 * 2 + 16)
        
        blockEncoder.encodeLong(Int64(blockObjectCount))
        blockEncoder.encodeBytes(bytes)
        blockEncoder.encodeFixed(sync)
        guard outputStream!.write(blockEncoder.bytes, maxLength: blockEncoder.bytes.count) == blockEncoder.bytes.count else {
            throw AvroError.errorWritting
        }
        byteCount += blockEncoder.bytes.count
        encoder = nil
        blockObjectCount = 0
    }
    
    open func close() throws {
        try closeBlock()

        guard outputStream != nil else {
            return
        }
        outputStream?.close()
    }
    
    open var outputData : Data? {
        guard outputStream != nil else {
            return nil
        }
        guard url == nil else {
            return nil
        }
        
        return outputStream!.property(forKey: Stream.PropertyKey.dataWrittenToMemoryStreamKey) as? Data
    }
    
    @discardableResult open func  tryToClose() -> Bool {
        do {
            try close()
        } catch {
            print("error while writing avro file")
            return false
        }
        return true
    }
    
    deinit {
        tryToClose()
    }
}
