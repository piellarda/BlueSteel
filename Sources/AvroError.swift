//
//  AvroError.swift
//  BlueSteel
//
//  Created by Jean-Alexis Montignies on 22.12.16.
//
//

import Foundation

enum AvroError : Error {
    case unsuportedURLType
    case errorCreatingFile
    case errorOpeningFileForReading
    case errorOpeningFileForWritting
    case errorCreatingDirectory
    case errorCreatingJSONSchema
    case errorWritting
    case errorEncodingHeader
    case errorEncodingObject
    case errorNotAnAvroFile
    case errorReadingFileSchema
    case errorReadingFileSync
}
