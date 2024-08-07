/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package com.justaddhippopotamus.ghr;

import com.justaddhippopotamus.ghr.RESP.IRESP;
import com.justaddhippopotamus.ghr.RESP.IRESPFactory;
import com.justaddhippopotamus.ghr.server.Server;
import com.justaddhippopotamus.ghr.server.Utils;
import com.justaddhippopotamus.ghr.server.commands.CommandJsonInterpreter;
import com.justaddhippopotamus.ghr.server.commands.ServerCommands;
import com.justaddhippopotamus.ghr.server.types.RedisGeo;
import com.justaddhippopotamus.ghr.server.types.RedisHyperLogLog;
import com.justaddhippopotamus.ghr.server.types.RedisString;
import org.junit.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.Provider;
import java.security.Security;
import java.util.*;

import static org.junit.Assert.*;
import com.justaddhippopotamus.ghr.RESP.*;

public class AppTest {
    public void testIRESPFactory() throws Exception {
        IRESPFactory factory = IRESPFactory.getDefault();
        IRESP val = factory.getNext(getStream("+String\r\n"));
        //Simple string works?
        assertTrue( val.getClass() == RESPSimpleString.class );
        assertTrue( ((RESPSimpleString) val).value.compareTo("String") == 0 );
        //Empty string/no bytes
        assertThrows( java.io.IOException.class, () -> factory.getNext(getStream("")));
        assertThrows( java.io.IOException.class, () -> factory.getNext(getStream("+String\r")));
        assertThrows( java.io.IOException.class, () -> factory.getNext(getStream("+String\n\r")));
        val = factory.getNext(getStream("$5\r\nhello\r\n"));
        assertTrue( val.getClass() == RESPBulkString.class);
        val = factory.getNext(getStream( ":1000\r\n"));
        assertTrue( val.getClass() == RESPInteger.class);
        assertThrows( java.io.IOException.class, () -> factory.getNext(getStream( ":fade\r\n")));
        assertThrows( java.io.IOException.class, () -> factory.getNext(getStream( ":0xFADE\r\n")));
        val = factory.getNext(getStream("$5\r\nhello\r\n"));
        assertTrue( val.getClass() == RESPBulkString.class);
        val = factory.getNext(getStream("$0\r\n\r\n"));
        assertTrue( val.getClass() == RESPBulkString.class);
        assertTrue( ((RESPBulkString) val).value.length == 0 );
        val = factory.getNext(getStream("$-1\r\n"));
        assertTrue( val.getClass() == RESPBulkString.class);
        assertTrue( ((RESPBulkString) val).value == null );
    }

    public void jsonIsh() throws Exception {
        ServerCommands c = new ServerCommands();
        c.init("commands/acl.json");
    }

    private InputStream getStream(String inputString) {
        return new ByteArrayInputStream(inputString.getBytes(Server.CHARSET) );
    }

    public void directory() throws Exception {
        InputStream is = getClass().getResourceAsStream("commands/");
        if( is != null ) {
            String str = new String(is.readAllBytes(), Server.CHARSET);
            System.out.println(str);
        } else {
            File dir = new File("src/main/resources/commands/");
            String [] list = dir.list();
            for( var a : list ) {
                System.out.println(a);
            }


        }
    }

    public void testIn() throws Exception {
        String path = "src/main/resources/commands/";
        File dir = new File(path);
        String [] list = dir.list();
        CommandJsonInterpreter jsonInterpreter = new CommandJsonInterpreter();
        for( var a : list ) {
            String fileName = path+a;
            InputStream is = new FileInputStream(fileName);
            jsonInterpreter.loadNew(is);
            is.close();
        }
        jsonInterpreter.linkSubcommands();
        System.out.println("done");
    }

    public void testServerCommandsRead() throws Exception {
        ServerCommands.init("commands/" );
        System.out.println("done");
    }


    @Test
    public void testGlob() throws Exception {
        Set<String> testHellos = Set.of("hello", "hallo", "hbllo", "hcllo", "heeeeello", "hcallo", "hdllo", "olleh", "Food");
        Set<String> currentResults = Utils.getMatches(testHellos,"h?llo");
                    currentResults = Utils.getMatches(testHellos, "h[ae]llo");
                    currentResults = Utils.getMatches(testHellos, "h[a-b]llo");
                    currentResults = Utils.getMatches(testHellos, "h[^e]llo");
                    currentResults = Utils.getMatches(testHellos, "h*llo");
    }
    public void testBitOps() throws Exception {
        byte [] fullSet = { (byte)0xFF, (byte)0x7F, (byte)0xFF };
        RedisString rs = new RedisString(fullSet);
        assertTrue(rs.bitcount(0,0,false) == 8 );
        assertTrue( rs.bitcount(0,-1, false) == 23 );
        assertTrue( rs.bitcount(0,-1,true) == 23 );
        assertTrue( rs.bitcount(0,7, true ) == 8 );
        assertTrue( rs.bitcount( 5, 11, true) == 6 );
        rs = new RedisString();
        rs.setbit(2,1);
        rs.setbit(3,1);
        rs.setbit(5,1);
        rs.setbit(10,1);
        rs.setbit(11,1);
        rs.setbit(14,1);
        assertTrue(rs.toString().compareTo("42") == 0 );
        rs.setbit( 5,0);
        assertTrue(rs.toString().compareTo("02") == 0 );
        assertTrue( rs.bitpos(1, 0, -1, false) == 2 );
        assertTrue(rs.bitpos(1,3,-1, true) == 3);
        assertTrue(rs.bitpos(0,3,-1,true) == 4);
        assertTrue( rs.bitpos(0,90,-1, false) == -1);
        byte [] first          = { (byte)0b10101010, (byte)0b01010101, (byte)0b00110011 };
        byte [] second         = { (byte)0b10101010, (byte)0b10101010, (byte)0b11110000 };
        byte [] firstANDsecond = { (byte)0b10101010, (byte)0b00000000, (byte)0b00110000 };
        byte [] firstORsecond  = { (byte)0b10101010, (byte)0b11111111, (byte)0b11110011 };
        byte [] third = { (byte)0xFF, (byte)0xFF, (byte)0x00, (byte)0x10 };

        RedisString rsFirst = new RedisString(first);
        RedisString rsSecond = new RedisString(second);
        RedisString rsThird = new RedisString(third);
        RedisString rsFirstAndSecond = new RedisString(firstANDsecond);
        RedisString rsFirstOrSecond = new RedisString(firstORsecond);

        RedisString rsBlankSlate = RedisString.redisStringLength(5);
        RedisString neverTouched = RedisString.redisStringLength(5);

        rsBlankSlate.or(rsFirst);
        assertTrue( rsBlankSlate.valuesSameUpToMinLength(rsFirst) );
        rsBlankSlate.and(null);
        assertTrue( rsBlankSlate.valuesSameUpToMinLength(neverTouched));
        rsBlankSlate.xor(rsFirst);
        assertTrue( rsBlankSlate.valuesSameUpToMinLength(rsFirst) );
        rsBlankSlate.and(rsSecond);
        assertTrue( rsBlankSlate.valuesSameUpToMinLength(rsFirstAndSecond) );
        rsBlankSlate.and(null);
        rsBlankSlate.or(rsFirst);
        rsBlankSlate.or(rsSecond);
        assertTrue( rsBlankSlate.valuesSameUpToMinLength(rsFirstOrSecond));
        RedisString rsFullSet = new RedisString( fullSet );
        assertTrue( rsFullSet.getLongValue( 5, 3, false ) == 7 );
        assertTrue( rsFullSet.getLongValue( 5, 3, true) == -1 );
        assertTrue( rsFullSet.getLongValue( 6, 3, true) == -2 );
        assertTrue( rsFullSet.getLongValue( 7, 3, true) == -3 );
        rsBlankSlate.and(null);
        rsFullSet = rsBlankSlate.not();
        byte [] setInt = {(byte)0xff, (byte)0x01, (byte)0x7f};
        RedisString setCompare = new RedisString(setInt);
        rsFullSet.setLongValue(0b000000010, 8, 9 );
        assertTrue( rsFullSet.valuesSameUpToMinLength(setCompare));

    }

    String [] toIgnore = {
    //"n_structure_single_eacute.json",
    //"n_array_a_invalid_utf8.json",
    //"n_structure_ascii-unicode-identifier.json",
    //"n_number_infinity.json",
    //"n_string_with_trailing_garbage.json",
    //"n_number_Inf.json",
    //"n_string_accentuated_char_no_quotes.json",
    //"n_number_.2e-3.json",
    //"n_structure_single_star.json",
    //"n_structure_lone-invalid-utf-8.json"
    };

    private static final char [] hexChars = new char [] {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
    private String sha1hex(byte [] input) {
        StringBuilder sb = new StringBuilder(42);
        for( var b : input ) {
            int b64 = ((int)b) & 0xFF;
            sb.append( hexChars[ b64 >> 4 ]);
            sb.append( hexChars[ b64 & 0x0F ]);
        }
        return sb.toString();
    }
    @Test
    public void testThing() throws Exception {
        Provider[] p = Security.getProviders();
        MessageDigest md = MessageDigest.getInstance("SHA1");

        //String sha1 = Base64.getEncoder().encodeToString(md.digest(luaCode.getBytes(Server.CHARSET)));
        String sha1 = sha1hex(md.digest("return".getBytes(StandardCharsets.US_ASCII)));

        String expected = "63143b6f8007b98c53ca2149822777b3566f9241";
    }

    private String maxPrint(byte [] toPrint) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for( int i = 0; i < toPrint.length; ++i ) {
            if( toPrint[i] != 0 ) {
                sb.append(i);
                sb.append(':');
                sb.append(Integer.toHexString(toPrint[i]&0xFF));
                sb.append(',');
            }
        }
        sb.append(']');
        return sb.toString();
    }

    @Test
    public void testHLL() throws Exception {
        RedisHyperLogLog hll = new RedisHyperLogLog();
        for( int i = 0; i < RedisHyperLogLog.HLL_REGISTERS; ++i ) {
            hll.setRegisterDense(i, (i+1) % RedisHyperLogLog.HLL_REGISTER_MAX);
        }
        for( int i = 0; i < RedisHyperLogLog.HLL_REGISTERS; ++i ) {
            hll.hllDenseSet(i, (i) % RedisHyperLogLog.HLL_REGISTER_MAX);
        }
        for( int i = 0; i < RedisHyperLogLog.HLL_REGISTERS; ++i ) {
            if( (i+1)%RedisHyperLogLog.HLL_REGISTER_MAX == 0 )
                assertTrue(hll.getRegisterDense(i)==(i)%RedisHyperLogLog.HLL_REGISTER_MAX);
            else
                assertTrue(hll.getRegisterDense(i)==(i+1)%RedisHyperLogLog.HLL_REGISTER_MAX);
        }

        RedisHyperLogLog hll2 = new RedisHyperLogLog();
        List<RESPBulkString> rbsList = List.of(new RESPBulkString("a"),new RESPBulkString("b"),new RESPBulkString("c"), new RESPBulkString("d"), new RESPBulkString("e"));
        hll2.hllAdd(rbsList);
        assertEquals(5, hll2.hllSelfCount());

        RedisHyperLogLog hll3 = new RedisHyperLogLog();
        RedisHyperLogLog hll4 = new RedisHyperLogLog();
        List<RESPBulkString> r1 = List.of(new RESPBulkString("0"),new RESPBulkString("1"));
        List<RESPBulkString> r2 = List.of(new RESPBulkString("2"),new RESPBulkString("3"));
        byte [] max = null;
        hll3.hllAdd(r1);
        System.out.println(hll3.toSparseString());
        hll4.hllAdd(r2);
        System.out.println(hll4.toSparseString());
        max = hll3.mergeMax(max);
        System.out.println(maxPrint(max));
        max = hll4.mergeMax(max);
        System.out.println(maxPrint(max));
        RedisHyperLogLog hll5 = new RedisHyperLogLog();
        hll5.setFromMax(max);
        System.out.println(hll5.toSparseString());
    }

    @Test
    public void TestGeo() throws IOException {
        //geoadd Sicily 13.361389 38.115556 Palermo 15.087269 37.502669 Catania
        var what = RedisGeo.geohashGetDistance(15,37, 13.361389,38.115556);
        RedisGeo rg = new RedisGeo();
        RedisGeo.GeoItem gi = new RedisGeo.GeoItem();
        gi.name = "Palmero";
        gi.longitude = 15.087269d;
        gi.latitude = 37.502669d;
        rg.addGeo(List.of(gi),false,false,false);
        gi.name = "Catania";
        gi.longitude = 13.361389d;
        gi.latitude = 38.115556d;
        rg.addGeo(List.of(gi),false,false,false);
        assertEquals(166274.1516d,rg.distance("Palmero","Catania","") ,0.0001d);
        var otherWhat = rg.find(15,37,200000);
        System.out.println(otherWhat.size());
    }
}
