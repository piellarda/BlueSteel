Pod::Spec.new do |spec|
  spec.name         = "BlueSteel"
  spec.version      = "3.0.1"
  spec.summary      = "An Avro encoding/decoding library for Swift."
  spec.homepage     = "https://github.com/piellarda/BlueSteel"
  spec.license      = { :type => "MIT", :file => "LICENSE" }
  spec.author    = "Antoine Piellard"
  spec.platform     = :ios
  spec.ios.deployment_target = '10.0'
  spec.swift_version = '4.2'
  spec.source       = { :git => "git@github.com:piellarda/BlueSteel.git", :tag => "#{spec.version}" }
  spec.source_files  = "Sources"
end