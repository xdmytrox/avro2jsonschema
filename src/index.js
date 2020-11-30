/**
 * @typedef {import("avsc").ForSchemaOptions} AvroForSchemaOptions
 * @typedef {import("avsc").schema.AvroSchema} AvroSchema
 * @typedef {import("avsc").Type} AvroType
 * @typedef {import("avsc").types.Field} AvroFieldType
 * @typedef {import("avsc").types.IntType} AvroIntType
 * @typedef {import("avsc").types.MapType} AvroMapType
 * @typedef {import("avsc").types.LongType} AvroLongType
 * @typedef {import("avsc").types.NullType} AvroNullType
 * @typedef {import("avsc").types.EnumType} AvroEnumType
 * @typedef {import("avsc").types.ArrayType} AvroArrayType
 * @typedef {import("avsc").types.BytesType} AvroBytesType
 * @typedef {import("avsc").types.FixedType} AvroFixedType
 * @typedef {import("avsc").types.FloatType} AvroFloatType
 * @typedef {import("avsc").types.RecordType} AvroRecordType
 * @typedef {import("avsc").types.StringType} AvroStringType
 * @typedef {import("avsc").types.DoubleType} AvroDoubleType
 * @typedef {import("avsc").types.BooleanType} AvroBooleanType
 * @typedef {import("avsc").types.LogicalType} AvroLogicalType
 * @typedef {import("avsc").types.UnwrappedUnionType} AvroUnwrappedUnionType
 * @typedef {import("avsc").types.WrappedUnionType} AvroWrappedUnionType
 */

const avro = require('avsc')

// TODO: MapConverter, LogicalConverter

/**
 * @this {Avro2JSONSchemaConverter}
 * @param {AvroLogicalType} type
 */
const decimal = function (type) {
  return { type: 'number' }
}

/**
 * @this {Avro2JSONSchemaConverter}
 * @param {AvroLogicalType} type
 */
const timestampMillis = function (type) {
  return { type: 'integer', minimum: 1, maximum: 2 ** 63 - 1 }
}


/**
 * @this {Avro2JSONSchemaConverter}
 * @param {AvroLogicalType} type
 */
const date = function (type) {
  return { type: 'integer', minimum: 1, maximum: 2 ** 63 - 1 }
}
  
/** @type {ConverterOptions['logicalTypes']} */
const defaultLogicalTypes = {
  decimal,
  date,
  'timestamp-millis': timestampMillis
}


/**
 * @typedef {Object} ConverterOptions
 * @property {Record<string, (AvroLogicalType) => Object>} [logicalTypes={}]
 */

class Avro2JSONSchemaConverter {
  /**
   * @param {ConverterOptions} options
   */
  constructor(options = {}) {
    this.setupLogicalTypes(options.logicalTypes)
  }

  /**
   * @param {AvroSchema} schema
   * @returns {Object}
   */
  convert(schema) {
    const type = avro.Type.forSchema(schema, {
      noAnonymousTypes: true,
      logicalTypes: this.logicalTypeFakeClasses
    })
    const convert = this.resolveConverter(type)
    return convert(type)
  }

  /**
   * @private
   * @param {ConverterOptions['logicalTypes']} logicalTypes
   */
  setupLogicalTypes(logicalTypes = {}) {
    /** @private */
    this.logicalTypeConvertors = Object.assign({}, defaultLogicalTypes, logicalTypes)
    
    /** @private */
    this.logicalTypeFakeClasses = Object.keys(this.logicalTypeConvertors).reduce((classes, logicalTypeName) => {
      classes[logicalTypeName] = class extends avro.types.LogicalType {}
      return classes
    }, {})
  }

  /**
   * @private
   * @param {AvroType} type
   * @returns {(type: AvroType) => Object}
   */
  resolveConverter (type) {
    if (type instanceof avro.types.LogicalType) {
      return this.logicalTypeConvertors[type.typeName.split(':')[1]].bind(this)
    }

    const typeName = type.constructor.name
    const fnName = `convert${typeName.replace('Type', '')}`
    return this[fnName].bind(this)
  }

  /**
   * @private
   * @param {AvroRecordType} type
   */
  convertRecord (type) {
    const schema = {
      type: 'object',
      properties: {},
      required: []
    }
    type.fields.forEach((field) => {
      const convert = this.resolveConverter(field.type)
      schema.properties[field.name] = convert(field.type)
      if (field.defaultValue() !== undefined) {
        schema.properties[field.name].default = field.defaultValue()
      } else {
        schema.required.push(field.name)
      }
    })
    return schema
  }

  /**
   * @private
   * @param {AvroEnumType} type
   */
  convertEnum (type) {
    return { type: 'string', enum: type.symbols.map(_ => _) }
  }

  /**
   * @private
   * @param {AvroArrayType} type
   */
  convertArray (type) {
    const convert = this.resolveConverter(type.itemsType)
    return { type: 'array', items: convert(type.itemsType) }
  }

  /**
   * @private
   * @param {AvroUnwrappedUnionType} type
   */
  convertUnwrappedUnion(type) { return this.convertUnion(type) }

  /**
   * @private
   * @param {AvroWrappedUnionType} type
   */
  convertWrappedUnion(type) { return this.convertUnion(type) }

  /**
   * @private
   * @param {AvroFixedType} type
   */
  convertFixed({ size }) { return this.convertBuffer(size, size) }

  /** @private */
  convertBytes() { return this.convertBuffer() }

  /** @private */
  convertFloat() { return { type: 'number' } }

  /** @private */
  convertDouble() { return this.convertFloat() }

  /** @private */
  convertBoolean() { return { type: 'boolean' } }

  /** @private */
  convertString() { return { type: 'string' } }

  /** @private */
  convertNull() { return { type: 'null' } }

  /** @private */
  convertLong() { return { type: 'integer', minimum: -(2 ** 63), maximum: 2 ** 63 - 1 } }

  /** @private */
  convertInt() { return { type: 'integer', minimum: -(2 ** 31), maximum: 2 ** 31 - 1 } }


  /**
   * @private
   * @param {AvroUnwrappedUnionType | AvroWrappedUnionType} type
   */
  convertUnion(type) {
    return {
      oneOf: type.types.map(type => {
        return this.resolveConverter(type)(type)
      })
    }
  }

  /**
   * @private
   * @param {number} [min]
   * @param {number} [max]
   */
  convertBuffer(min, max) {
    return Object.assign(
      { type: 'string', pattern: '^[\u0000-\u00ff]*$' },
      max !== undefined && { maxLength: max },
      min !== undefined && { minLength: min }
    )
  }


}

module.exports = {
  Avro2JSONSchemaConverter
}