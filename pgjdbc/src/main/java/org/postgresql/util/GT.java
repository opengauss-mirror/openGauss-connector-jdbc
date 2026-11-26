/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * All rights reserved.
 *
 * Licensed under the 2-Clause BSD License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/license/bsd-2-clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.postgresql.util;
import org.postgresql.log.Logger;
import org.postgresql.log.Log;

import java.text.MessageFormat;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 * This class provides a wrapper around a gettext message catalog that can provide a localized
 * version of error messages. The caller provides a message String in the standard
 * java.text.MessageFormat syntax and any arguments it may need. The returned String is the
 * localized version if available or the original if not.
 */
public class GT {

  private static final GT _gt = new GT();
  private static final Object[] noargs = new Object[0];
  private static Log LOGGER = Logger.getLogger(GT.class.getName());

  public static String tr(String message, Object... args) {
    return _gt.translate(message, args);
  }

  private ResourceBundle _bundle;

  private GT() {
    try {
      _bundle = ResourceBundle.getBundle("org.postgresql.translation.messages");
      //#endif
    } catch (MissingResourceException mre) {
      // translation files have not been installed
      _bundle = null;
    }
  }

  private String translate(String message, Object[] args) {
    if (_bundle != null && message != null) {
      try {
        message = _bundle.getString(message);
      } catch (MissingResourceException mre) {
          // If we can't find a translation, just
          // use the untranslated message.
          LOGGER.trace("Catch MissingResourceException while translation. ", mre);
      }
    }

    // If we don't have any parameters we still need to run
    // this through the MessageFormat(ter) to allow the same
    // quoting and escaping rules to be used for all messages.
    //
    if (args == null) {
      args = noargs;
    }

    // Replace placeholders with arguments
    //
    if (message != null) {
      message = MessageFormat.format(message, args);
    }

    return message;
  }
}
