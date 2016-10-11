//
//  AvroFileContainer.swift
//  BlueSteel
//
//  Created by Jean-Alexis Montignies on 07.10.16.
//
//

open class AvroFileContainer {
    enum AvroFileContainerError : Error {
        case unsuportedURLType
        case errorCreatingFile
        case errorCreatingDirectory
        case errorCreatingJSONSchema
        case errorEncodingHeader
        case errorEncodingObject
    }
    
    static let magic: [UInt8] = [0x4f, 0x62, 0x6a, 0x01] // Obj\0x01
    static let avroFileContainerSchema = Schema(
        "{\"type\": \"record\", \"name\": \"org.apache.avro.file.Header\"," +
            "\"fields\" : [" +
            "{\"name\": \"magic\", \"type\": {\"type\": \"fixed\", \"name\": \"Magic\", \"size\": 4}}," +
            "{\"name\": \"meta\", \"type\": {\"type\": \"map\", \"values\": \"bytes\"}}, " +
        "{\"name\": \"sync\", \"type\": {\"type\": \"fixed\", \"name\": \"Sync\", \"size\": 16}}, ]}")

    var URL: URL!
    var fileHandle: FileHandle?
    var schema: Schema!
    var error: Error?
    let sync: [UInt8] = AvroFileContainer.randomSync()
    var encoder : AvroEncoder?
    var estimatedEncodedObjectLength = 0
    var blockSize = 100000
    var blockObjectCount = 0
    
    static func randomSync() -> [UInt8] {
        var sync : [UInt8] = []
        sync.reserveCapacity(16)
        for _ in 0...15 {
            sync.append(UInt8(arc4random_uniform(255)))
        }
        return sync
    }
    
    
    public init(schema: Schema, URL: URL) {
        self.schema = schema
        self.URL = URL
    }
    
    func openFileAndWriteHeader() throws {
        guard URL.isFileURL else {
            throw AvroFileContainerError.unsuportedURLType
        }
        let fileManager = FileManager.default
        let path = URL.path

        do {
            try fileManager.createDirectory(atPath: (path as NSString).deletingLastPathComponent, withIntermediateDirectories: true, attributes: nil)
        }
        catch  {
            throw AvroFileContainerError.errorCreatingDirectory
        }
        
    
        if !fileManager.createFile(atPath: path, contents: nil, attributes: nil) {
            throw AvroFileContainerError.errorCreatingFile
        }
        
        guard let schemaJson = schema.json() else {
            throw AvroFileContainerError.errorCreatingJSONSchema
        }
        
        let metaData = [
            "avro.schema" : AvroValue.avroStringValue(schemaJson)
        ]
        let headerFields = [
            "magic" : AvroValue.avroFixedValue(AvroFileContainer.magic),
            "meta" : AvroValue.avroMapValue(metaData),
            "sync" : AvroValue.avroFixedValue(sync)
        ]
        
        let header = AvroValue.avroRecordValue(headerFields)
        guard let encodedHeader = header.encode(AvroFileContainer.avroFileContainerSchema) else {
            throw AvroFileContainerError.errorEncodingHeader
        }

        fileHandle = try FileHandle(forUpdating: URL)
        
        fileHandle?.write(Data(encodedHeader))
    }
    
    open func append(value: AvroValue) throws {
        guard error == nil else {
            throw error!
        }
        
        if fileHandle == nil {
            try openFileAndWriteHeader()
        }
        
        if encoder == nil {
            encoder = AvroEncoder(capacity: blockSize)
        }
        
        let previousByteCount = encoder!.bytes.count
        guard let bytes = value.encode(encoder!, schema: schema) else {
            throw AvroFileContainerError.errorEncodingObject
        }
        
        blockObjectCount += 1
        
        let objectEncodedLength = bytes.count - previousByteCount
        estimatedEncodedObjectLength = objectEncodedLength > estimatedEncodedObjectLength ? objectEncodedLength : estimatedEncodedObjectLength
        
        if bytes.count + estimatedEncodedObjectLength > blockSize {
            closeBlock()
        }
    }
    
    func closeBlock() {
        guard encoder != nil else {
            return
        }
        let bytes = encoder!.bytes
        let blockEncoder = AvroEncoder(capacity: encoder!.bytes.count + 10 * 2 + 16)
        
        blockEncoder.encodeLong(Int64(blockObjectCount))
        blockEncoder.encodeBytes(bytes)
        blockEncoder.encodeFixed(sync)
        fileHandle!.write(Data(blockEncoder.bytes))
        
        encoder = nil
        blockObjectCount = 0
    }
    
    open func close() {
        guard fileHandle != nil else {
            return
        }
        closeBlock()
        fileHandle?.closeFile()
    }
    
    deinit {
        close()
    }
}
