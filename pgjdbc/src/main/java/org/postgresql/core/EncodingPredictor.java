/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.core;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>Predicts encoding for error messages based on some heuristics.</p>
 *
 * <ol>
 * <li>For certain languages, it is known how "FATAL" is translated</li>
 * <li>For Japanese, several common words are hardcoded</li>
 * <li>Then try various LATIN encodings</li>
 * </ol>
 */
public class EncodingPredictor {
  /**
   * In certain cases the encoding is not known for sure (e.g. before authentication).
   * In such cases, backend might send messages in "native to database" encoding,
   * thus pgjdbc has to guess the encoding nad
   */
  public static class DecodeResult {
    public final String result;
    public final String encoding; // JVM name

    DecodeResult(String result, String encoding) {
      this.result = result;
      this.encoding = encoding;
    }
  }
  private static final Logger LOGGER = Logger.getLogger(EncodingPredictor.class.getName());

  static class Translation {
    public final String fatalText;
    private final String[] texts;
    public final String language;
    public final String[] encodings;

    Translation(String fatalText, String[] texts, String language, String... encodings) {
      this.fatalText = fatalText;
      this.texts = texts;
      this.language = language;
      this.encodings = encodings;
    }
  }

  private static final Translation[] FATAL_TRANSLATIONS =
      new Translation[]{
          new Translation("ВАЖНО", null, "ru", "WIN", "ALT", "KOI8"),
          new Translation("KATASTROFALNY", null, "pl", "LATIN2"),
          new Translation("FATALE", null, "it", "LATIN1", "LATIN9"),
          new Translation(null, null, "fr/de/es/pt_BR", "LATIN1", "LATIN3", "LATIN4", "LATIN5",
              "LATIN7", "LATIN9"),
      };

  public static DecodeResult decode(byte[] bytes, int offset, int length) {
    Encoding defaultEncoding = Encoding.defaultEncoding();
    for (Translation tr : FATAL_TRANSLATIONS) {
      for (String encoding : tr.encodings) {
        Encoding encoder = Encoding.getDatabaseEncoding(encoding);
        if (encoder == defaultEncoding) {
          continue;
        }

        // If there is a translation for "FATAL", then try typical encodings for that language
        if (tr.fatalText != null) {
          byte[] encoded;
          try {
            byte[] tmp = encoder.encode(tr.fatalText);
            encoded = new byte[tmp.length + 2];
            encoded[0] = 'S';
            encoded[encoded.length - 1] = 0;
            System.arraycopy(tmp, 0, encoded, 1, tmp.length);
          } catch (IOException e) {
            continue;// should not happen
          }

          if (!arrayContains(bytes, offset, length, encoded, 0, encoded.length)) {
            continue;
          }
        }

        // No idea how to tell Japanese from Latin languages, thus just hard-code certain Japanese words
        if (tr.texts != null) {
          boolean foundOne = false;
          for (String text : tr.texts) {
            try {
              byte[] textBytes = encoder.encode(text);
              if (arrayContains(bytes, offset, length, textBytes, 0, textBytes.length)) {
                foundOne = true;
                break;
              }
            } catch (IOException e) {
                // do not care, will try other encodings
                LOGGER.log(Level.FINEST, "Catch IOException while encoding. ", e);
            }
          }
          if (!foundOne) {
            // Error message does not have key parts, will try other encodings
            continue;
          }
        }

        try {
          String decoded = encoder.decode(bytes, offset, length);
          if (decoded.indexOf(65533) != -1) {
            // bad character in string, try another encoding
            continue;
          }
          return new DecodeResult(decoded, encoder.name());
        } catch (IOException e) {
            // do not care
            LOGGER.log(Level.FINEST, "Catch IOException while decoding. ", e);
        }
      }
    }
    return null;
  }

  private static boolean arrayContains(
      byte[] first, int firstOffset, int firstLength,
      byte[] second, int secondOffset, int secondLength
  ) {
    if (firstLength < secondLength) {
      return false;
    }

    for (int i = 0; i < firstLength; i++) {
      for (; i < firstLength && first[firstOffset + i] != second[secondOffset]; i++) {
        // find the first matching byte
      }

      int j = 1;
      for (; j < secondLength && first[firstOffset + i + j] == second[secondOffset + j]; j++) {
        // compare arrays
      }
      if (j == secondLength) {
        return true;
      }
    }
    return false;
  }
}
