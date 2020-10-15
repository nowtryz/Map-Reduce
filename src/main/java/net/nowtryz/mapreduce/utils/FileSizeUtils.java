package net.nowtryz.mapreduce.utils;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;

public class FileSizeUtils {
    /**
     * convert byte count to readable
     * @see <a href="https://stackoverflow.com/questions/3758606/how-can-i-convert-byte-size-into-a-human-readable-format-in-java">On Stackoverflow</a>
     * @param bytes the file size
     * @return the file size in a human readable format
     */
    public static String toHumanReadableSize(long bytes) {
        long absB = bytes == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(bytes);
        if (absB < 1024) {
            return bytes + " B";
        }
        long value = absB;
        CharacterIterator ci = new StringCharacterIterator("KMGTPE");
        for (int i = 40; i >= 0 && absB > 0xfffccccccccccccL >> i; i -= 10) {
            value >>= 10;
            ci.next();
        }
        value *= Long.signum(bytes);
        return String.format("%.1f %ciB", value / 1024.0, ci.current());
    }
}
