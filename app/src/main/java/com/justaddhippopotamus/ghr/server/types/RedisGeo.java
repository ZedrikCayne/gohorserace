package com.justaddhippopotamus.ghr.server.types;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;

import java.util.*;
import java.util.stream.Collectors;

public class RedisGeo extends RedisSortedSet {
    public static final int XYLONG = 1;
    public static final int XYLAT = 0;
    public static final char prefix = 'G';

    public static final int GEO_STEP_MAX = 26; /* 26*2 = 52 bits. */

    /* Limits from EPSG:900913 / EPSG:3785 / OSGEO:41001 */
    public static final double GEO_REAL_LAT_MIN = -90.0;
    public static final double GEO_REAL_LAT_MAX = 90.0;
    public static final double GEO_LAT_MIN = -85.05112878;
    public static final double GEO_LAT_MAX = 85.05112878;
    public static final double GEO_LONG_MIN = -180;
    public static final double GEO_LONG_MAX = 180;

    private enum GeoDirection {
        GEOHASH_NORTH,
        GEOHASH_EAST,
        GEOHASH_WEST,
        GEOHASH_SOUTH,
        GEOHASH_SOUTH_WEST,
        GEOHASH_SOUTH_EAST,
        GEOHASH_NORTH_WEST,
        GEOHASH_NORTH_EAST
    }

    public static class GeoItem {
        public String name;
        public double longitude;
        public double latitude;
    }

    private static final long [] B = {0x5555555555555555L, 0x3333333333333333L,
            0x0F0F0F0F0F0F0F0FL, 0x00FF00FF00FF00FFL,
            0x0000FFFF0000FFFFL, 0x00000000FFFFFFFFL};
    private static final int [] S = {1, 2, 4, 8, 16};
    private static final int [] S2 = {0,1,2,4,8,16};
    public static long interleave64(double xlo, double ylo) {
        long x = (long)xlo;
        long y = (long)ylo;

        x = (x | (x << S[4])) & B[4];
        y = (y | (y << S[4])) & B[4];

        x = (x | (x << S[3])) & B[3];
        y = (y | (y << S[3])) & B[3];

        x = (x | (x << S[2])) & B[2];
        y = (y | (y << S[2])) & B[2];

        x = (x | (x << S[1])) & B[1];
        y = (y | (y << S[1])) & B[1];

        x = (x | (x << S[0])) & B[0];
        y = (y | (y << S[0])) & B[0];

        return x | (y << 1);
    }

    public static void deinterleave64(double interleaved, double [] xy) {
        long x = (long)interleaved;
        long y = ((long)interleaved) >> 1;

        x = (x | (x >>> S2[0])) & B[0];
        y = (y | (y >>> S2[0])) & B[0];

        x = (x | (x >>> S2[1])) & B[1];
        y = (y | (y >>> S2[1])) & B[1];

        x = (x | (x >>> S2[2])) & B[2];
        y = (y | (y >>> S2[2])) & B[2];

        x = (x | (x >>> S2[3])) & B[3];
        y = (y | (y >>> S2[3])) & B[3];

        x = (x | (x >>> S2[4])) & B[4];
        y = (y | (y >>> S2[4])) & B[4];

        x = (x | (x >>> S2[5])) & B[5];
        y = (y | (y >>> S2[5])) & B[5];

        xy[XYLAT] = x;
        xy[XYLONG] = y;
    }

    public static void degeohash(double score, double [] xy) {
        double [] oxy = {0,0};
        deinterleave64(score,oxy);
        final double lat_scale = GEO_LAT_MAX - GEO_LAT_MIN;
        final double long_scale = GEO_LONG_MAX - GEO_LONG_MIN;

        double a = (oxy[XYLAT]) / (1<<GEO_STEP_MAX) * lat_scale + GEO_LAT_MIN;
        double b = (oxy[XYLAT]+1.0d) / (1<<GEO_STEP_MAX) * lat_scale + GEO_LAT_MIN;
        xy[XYLAT] = (a+b) / 2.0f;
        double c = (oxy[XYLONG]) / (1<<GEO_STEP_MAX) * long_scale + GEO_LONG_MIN;
        double d = (oxy[XYLONG]+1.0d) / (1<<GEO_STEP_MAX) * long_scale + GEO_LONG_MIN;
        xy[XYLONG] = (c+d) / 2.0f;
    }


    public static double geohash(double longitude, double latitude) {
        if( longitude < GEO_LONG_MIN || longitude > GEO_LONG_MAX ||
            latitude > GEO_LAT_MAX || latitude < GEO_LAT_MIN )
            throw new RuntimeException("Latitude/Longitude out of range.");

        double longOffset = (longitude - GEO_LONG_MIN) / (GEO_LONG_MAX - GEO_LONG_MIN) * (double)(1L << GEO_STEP_MAX);
        double latOffset = (latitude - GEO_LAT_MIN) / (GEO_LAT_MAX - GEO_LAT_MIN) * (double)(1L<<GEO_STEP_MAX);

        long interleaved = interleave64(latOffset,longOffset);

        return interleaved;
    }
    public static double geohashReal(double originalScore) {
        double [] xy = {0,0};
        degeohash(originalScore,xy);
        double longOffset = (xy[XYLONG] - GEO_LONG_MIN) / (GEO_LONG_MAX - GEO_LONG_MIN) * (double)(1L<<GEO_STEP_MAX);
        double latOffset = (xy[XYLAT] - GEO_REAL_LAT_MIN) / (GEO_REAL_LAT_MAX - GEO_REAL_LAT_MIN) * (double)(1L<<GEO_STEP_MAX);
        return interleave64(latOffset,longOffset);
    }

    public static final byte [] geoalphabet = {'0','1','2','3','4','5','6',
            '7', '8','9','b','c','d','e','f','g','h','j','k','m','n',
            'p','q','r','s','t','u','v','w','x','y','z'};

    public static byte [] geoHashToBytes(double score) {
        byte [] buf = new byte [11];
        long bits = (long)score;
        for (int i = 0; i < 11; i++) {
            int idx;
            if (i == 10) {
                /* We have just 52 bits, but the API used to output
                 * an 11 bytes geohash. For compatibility we assume
                 * zero. */
                idx = 0;
            } else {
                idx = (int)((bits >>> (52-((i+1)*5))) & 0x1f);
            }
            buf[i] = geoalphabet[idx];
        }
        return buf;
    }

    public synchronized int addGeo(List<GeoItem> stuff, boolean NX, boolean XX, boolean CH) {
        Map<String,Double> stuffToAdd = new HashMap<>(stuff.size());
        stuff.forEach(s -> {
            stuffToAdd.put(s.name,geohash(s.longitude,s.latitude));
        });
        return add(stuffToAdd,NX,XX,false,false,CH);
    }
    private static final double D_R = (Math.PI/ 180.0);
    private static final double R_MAJOR = 6378137.0;
    private static final double R_MINOR = 6356752.3142;
    private static final double RATIO = (R_MINOR / R_MAJOR);
    private static final double ECCENT = (Math.sqrt(1.0 - (RATIO *RATIO)));
    private static final double COM = (0.5 * ECCENT);
    private static double deg_rad(double ang) { return ang * D_R; }
    private static double rad_deg(double ang) { return ang / D_R; }

    int geohashEstimateStepsByRadius(double range_meters, double lat) {
        if (range_meters == 0) return 26;
        int step = 1;
        while (range_meters < MERCATOR_MAX) {
            range_meters *= 2;
            step++;
        }
        step -= 2; /* Make sure range is included in most of the base cases. */

        /* Wider range towards the poles... Note: it is possible to do better
         * than this approximation by computing the distance between meridians
         * at this latitude, but this does the trick for now. */
        if (lat > 66 || lat < -66) {
            step--;
            if (lat > 80 || lat < -80) step--;
        }

        /* Frame to valid range. */
        if (step < 1) step = 1;
        if (step > 26) step = 26;
        return step;
    }


    /// @brief The usual PI/180 private static finalant
    private static final double DEG_TO_RAD = 0.017453292519943295769236907684886;
    /// @brief Earth's quatratic mean radius for WGS-84
    private static final double EARTH_RADIUS_IN_METERS = 6372797.560856;

    private static final double MERCATOR_MAX = 20037726.37;
    private static final double MERCATOR_MIN = -20037726.37;

    public synchronized double distance(String one, String two, String unit) {
        if( !value.containsKey(one) || !value.containsKey(two) )
            return -1.0d;

        double oneD = value.get(one).score;
        double twoD = value.get(two).score;

        double [] xy = {0,0};
        double [] xy2 = {0,0};

        degeohash(oneD,xy);
        degeohash(twoD,xy2);

        return convertDistance(geohashGetDistance(xy[XYLONG],xy[XYLAT],xy2[XYLONG],xy2[XYLAT]),unit);
    }
    public static double distanceToMeters(double d, String unit) {
        switch(unit) {
            case "MI":
                return d*1609.34d;
            case "KM":
                return d*1000.0d;
            case "FT":
                return d*0.3048d;
            case "M":
            case "":
                return d;
            default:
                throw new RuntimeException("Bad unit of measurement.");
        }
    }

    public static double convertDistance(double d, String unit) {
        switch(unit) {
            case "MI":
                return d/1609.34d;
            case "KM":
                return d/1000.0d;
            case "FT":
                return d/0.3048d;
            case "M":
            case "":
                return d;
            default:
                throw new RuntimeException("Bad unit of measurement.");
        }
    }
    static double geohashGetLatDistance(double lat1d, double lat2d) {
        return EARTH_RADIUS_IN_METERS * Math.abs(deg_rad(lat2d) - deg_rad(lat1d));
    }
    /* Calculate distance using haversine great circle distance formula. */
    public static double geohashGetDistance(double lon1d, double lat1d, double lon2d, double lat2d) {
        double lat1r, lon1r, lat2r, lon2r, u, v, a;
        lon1r = deg_rad(lon1d);
        lon2r = deg_rad(lon2d);
        v = Math.sin((lon2r - lon1r) / 2);
        /* if v == 0 we can avoid doing expensive math when lons are practically the same */
        if (v == 0.0)
            return geohashGetLatDistance(lat1d, lat2d);
        lat1r = deg_rad(lat1d);
        lat2r = deg_rad(lat2d);
        u = Math.sin((lat2r - lat1r) / 2);
        a = u * u + Math.cos(lat1r) * Math.cos(lat2r) * v * v;
        return 2.0 * EARTH_RADIUS_IN_METERS * Math.asin(Math.sqrt(a));
    }

    public static double distance(double longitude, double latitude, double hash, double [] xy) {
        degeohash(hash,xy);
        return geohashGetDistance(longitude,latitude,xy[XYLONG],xy[XYLAT]);
    }

    public static class SetDistancePosition implements Comparable<SetDistancePosition> {
        public String key;
        public long hash;
        public double distance;
        public double latitude, longitude;
        public SetDistancePosition(SetValue sv, double longitude, double latitude) {
            double [] xy = {0,0};
            degeohash(sv.score,xy);
            this.distance = geohashGetDistance(longitude,latitude,xy[XYLONG],xy[XYLAT]);
            this.longitude = xy[XYLONG];
            this.latitude = xy[XYLAT];
            this.key = sv.key;
            this.hash = (long)sv.score;
        }
        @Override
        public int compareTo(SetDistancePosition o) {
            var c = Double.compare(distance,o.distance);
            return c==0?key.compareTo(o.key):c;
        }
        public int inverseCompareTo(SetDistancePosition o) {
            return o.compareTo(this);
        }
    }

    public List<SetDistancePosition> find(double longitude, double latitude, double radius) {
        return sortedValue.stream()
                .map(sv -> new SetDistancePosition(sv,longitude,latitude))
                .filter( sv2 -> sv2.distance < radius)
                .collect(Collectors.toList());
    }
    public static boolean inRectangle(double width_m, double height_m,
                                      double centerX, double centerY,
                                      double targetX, double targetY ) {
        /* latitude distance is less expensive to compute than longitude distance
         * so we check first for the latitude condition */
        double lat_distance = geohashGetLatDistance(targetY, centerY);
        if (lat_distance > height_m/2) {
            return false;
        }
        double lon_distance = geohashGetDistance(targetX, targetY, centerX, targetY);
        if (lon_distance > width_m/2) {
            return false;
        }
        return true;
    }
    public List<SetDistancePosition> findBox(double longitude, double latitude, double width, double height) {
        return sortedValue.stream()
                .map(sv -> new SetDistancePosition(sv,longitude,latitude))
                .filter( sv2 -> inRectangle(width,height,longitude,latitude,sv2.longitude,sv2.latitude))
                .collect(Collectors.toList());
    }
}
