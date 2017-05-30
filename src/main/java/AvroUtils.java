import edu.zuhaib.avro.Person;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

/**
 * Created by Zuhaib on 5/30/2017.
 */
public class AvroUtils {

    public static void main(String[] args){

        GenerateData generateData = new GenerateData().invoke();
        Person person1 = generateData.getP1();
        Person person2 = generateData.getP2();
        File avroOutput = writeAvroFile(person1, person2);
        readAvroFile(avroOutput);
    }

    private static void readAvroFile(File avroOutput) {
        //Reading Avro file
        try {
        DatumReader<Person> personSpecificDatumReader = new SpecificDatumReader<Person>(Person.class);
        DataFileReader<Person> personDataFileReader = new DataFileReader<Person>(avroOutput, personSpecificDatumReader);
        Person p = null;
        while(personDataFileReader.hasNext()){
            p = personDataFileReader.next(p);
            System.out.println(p);
        }
    } catch(IOException e) {System.out.println("Error reading Avro");}
    }

    private static File writeAvroFile(Person person1, Person person2) {
        //Writing to disk
        File avroOutput = new File("person.avro");
        try {
            DatumWriter<Person> personSpecificDatumWriter = new SpecificDatumWriter<Person>(Person.class);
            DataFileWriter<Person> personDataFileWriter = new DataFileWriter<Person>(personSpecificDatumWriter);
            personDataFileWriter.create(person1.getSchema(), avroOutput);
            personDataFileWriter.append(person1);
            personDataFileWriter.append(person2);
            personDataFileWriter.close();
        } catch (IOException e) {System.out.println("Error writing Avro");}
        return avroOutput;
    }

    private static class GenerateData {
        private Person p1;
        private Person p2;

        public Person getP1() {
            return p1;
        }

        public Person getP2() {
            return p2;
        }

        public GenerateData invoke() {
            p1 = new Person();
            p1.setId(1);
            p1.setUsername("zans");
            p1.setFirstName("Mohammad Zuhaib");
            p1.setLastName("Ansari");
            p1.setBirthdate("1989-01-07");

            p2 = new Person();
            p2.setId(2);
            p2.setUsername("khall");
            p2.setFirstName("Ken");
            p2.setMiddleName("H");
            p2.setLastName("Hall");
            p2.setBirthdate("1969-12-04");
            return this;
        }
    }
}
