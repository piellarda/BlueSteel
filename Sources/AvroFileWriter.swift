//
//  AvroFileWriter.swift
//  BlueSteel
//
//  Created by Jean-Alexis Montignies on 07.10.16.
//
//

open class AvroFileWriter {
    
    static let magic: [UInt8] = [0x4f, 0x62, 0x6a, 0x01] // Obj\0x01
    static let avroFileContainerSchema = Schema(
        "{\"type\": \"record\", \"name\": \"org.apache.avro.file.Header\"," +
            "\"fields\" : [" +
            "{\"name\": \"magic\", \"type\": {\"type\": \"fixed\", \"name\": \"Magic\", \"size\": 4}}," +
            "{\"name\": \"meta\", \"type\": {\"type\": \"map\", \"values\": \"bytes\"}}, " +
        "{\"name\": \"sync\", \"type\": {\"type\": \"fixed\", \"name\": \"Sync\", \"size\": 16}}, ]}")

    var url: URL!
    var fileHandle: FileHandle?
    var schema: Schema!
    let sync: [UInt8] = AvroFileWriter.randomSync()
    var encoder : AvroEncoder?
    var estimatedEncodedObjectLength = 0
    var blockSize = 100000
    var blockObjectCount = 0
    public var objectCount = 0
    
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
    
    func openFileAndWriteHeader() throws {
        guard url.isFileURL else {
            throw AvroError.unsuportedURLType
        }
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
        
        guard let schemaJson = schema.json() else {
            throw AvroError.errorCreatingJSONSchema
        }
        
        let metaData = [
            "avro.schema" : AvroValue.avroStringValue(schemaJson)
        ]
        let headerFields = [
            "magic" : AvroValue.avroFixedValue(AvroFileWriter.magic),
            "meta" : AvroValue.avroMapValue(metaData),
            "sync" : AvroValue.avroFixedValue(sync)
        ]
        
        let header = AvroValue.avroRecordValue(headerFields)
        guard let encodedHeader = header.encode(AvroFileWriter.avroFileContainerSchema) else {
            throw AvroError.errorEncodingHeader
        }

        fileHandle = try FileHandle(forUpdating: url)
        
        fileHandle?.write(Data(encodedHeader))
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
        
        if fileHandle == nil {
            try openFileAndWriteHeader()
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
    
    open func close() throws {
        try closeBlock()

        guard fileHandle != nil else {
            return
        }
        fileHandle?.closeFile()
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
