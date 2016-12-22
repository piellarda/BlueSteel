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
    case errorCreatingDirectory
    case errorCreatingJSONSchema
    case errorEncodingHeader
    case errorEncodingObject
}
