package com.sample.avro;

import com.example.practice.AvroSchemaConverter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.kitesdk.data.spi.JsonUtil;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;


public class AvroConverter {

    /**
     * main method
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

       // String json = "{\"username\":\"miguno\",\"tweet\":\"Rock: Nerf paper, scissors is fine.\",\"timestamp\": 1366150681 }";
        String schemastr = getAvroSchema();//"{ \"type\" : \"record\", \"name\" : \"twitter_schema\", \"namespace\" : \"com.miguno.avro\", \"fields\" : [ { \"name\" : \"username\", \"type\" : \"string\", \"doc\"  : \"Name of the user account on Twitter.com\" }, { \"name\" : \"tweet\", \"type\" : \"string\", \"doc\"  : \"The content of the user's Twitter message\" }, { \"name\" : \"timestamp\", \"type\" : \"long\", \"doc\"  : \"Unix epoch time in seconds\" } ], \"doc:\" : \"A basic schema for storing Twitter messages\" }";

        /*byte[] avroByteArray = fromJsonToAvro(json,schemastr);

        Schema schema = Schema.parse(schemastr);
        DatumReader<GenericRecord> reader1 = new GenericDatumReader<GenericRecord>(schema);

        Decoder decoder1 = DecoderFactory.get().binaryDecoder(avroByteArray, null);
        GenericRecord result = reader1.read(null, decoder1);

        System.out.println(result.get("username").toString());
        System.out.println(result.get("tweet").toString());
        System.out.println(result.get("timestamp"));*/

        String json = "{\n" +
                "    \"id\": 1,\n" +
                "    \"name\": \"A green door\",\n" +
                "    \"price\": 12.50,\n" +
                "    \"tags\": [\"home\", \"green\"]\n" +
                "}\n"
                ;
        /*String avroSchema = JsonUtil.inferSchema(JsonUtil.parse(json), "myschema").toString();
        System.out.println(avroSchema);*/
        System.out.println( new AvroSchemaConverter(new ObjectMapper()).convert(json));

    }

    /**
     * @param json
     * @param schemastr
     * @throws Exception
     */
    static byte[] fromJsonToAvro(String json, String schemastr) throws Exception {

        InputStream input = new ByteArrayInputStream(json.getBytes());
        DataInputStream din = new DataInputStream(input);

        Schema schema = Schema.parse(schemastr);

        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);

        DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
        Object datum = reader.read(null, decoder);


        GenericDatumWriter<Object>  w = new GenericDatumWriter<Object>(schema);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        Encoder e = EncoderFactory.get().binaryEncoder(outputStream, null);

        w.write(datum, e);
        e.flush();

        return outputStream.toByteArray();
    }

    /**
     * getJsonBody
     * @return
     * @throws IOException
     */
    private static String getJsonBody() throws IOException {
        String jsonBody = new String(Files.readAllBytes(Paths.get("src/main/resources/input_msg.json")));
        return jsonBody;
    }

    /**
     * getAvroSchema
     * @return
     * @throws IOException
     */
    private static String getAvroSchema() throws IOException {
        String avroSchema = new String(Files.readAllBytes(Paths.get("src/main/resources/avro_schema.avsc")));
        return avroSchema;
    }
}