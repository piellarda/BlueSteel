//
//  AvroFileContainer.swift
//  BlueSteel
//
//  Created by Jean-Alexis Montignies on 04.01.17.
//
//

import Foundation

class AvroFileContainer {
    static let magic: [UInt8] = [0x4f, 0x62, 0x6a, 0x01] // Obj\0x01
    static let avroFileContainerSchema = Schema(
        "{\"type\": \"record\", \"name\": \"org.apache.avro.file.Header\"," +
            "\"fields\" : [" +
            "{\"name\": \"magic\", \"type\": {\"type\": \"fixed\", \"name\": \"Magic\", \"size\": 4}}," +
            "{\"name\": \"meta\", \"type\": {\"type\": \"map\", \"values\": \"bytes\"}}, " +
        "{\"name\": \"sync\", \"type\": {\"type\": \"fixed\", \"name\": \"Sync\", \"size\": 16}}, ]}")!

    static let metaDataSchemaKey = "avro.schema"
    static let headerMetaDataKey = "meta"
    static let headerMagicKey = "magic"
    static let headerSyncKey = "sync"
}
