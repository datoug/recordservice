// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.analysis;

import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Preconditions;

/**
 * An InputSplit filter, specified as part of a select list plan hint.
 * Used to filter table data to only include results from a specific file
 * and/or file offset.
 * The filter string is passed as a string in the form:
 * <file_name>@<offset>@<length> and this class parses the result.
 */
public class InputSplitFilter {
  private final String filterStr_;

  // Set during analyze()
  private String fileName_;
  private long offset_ = 0;
  private long length_ = 0;

  public InputSplitFilter(String filterStr) {
    Preconditions.checkNotNull(filterStr);
    filterStr_ = filterStr;
  }

  /**
   * Parses the input.
   */
  public void analyze(Analyzer analyzer) throws AnalysisException {
    // TODO: Probably want a better way to represent this.
    String[] parts = filterStr_.split("@");
    if (parts.length != 3) {
      throw new AnalysisException("InputSplit filter must be in form: " +
          "<file_name>@<offset>@<length>");
    }
    fileName_ = parts[0];
    offset_ = Long.parseLong(parts[1]);
    length_ = Long.parseLong(parts[2]);
  }

  public long getLength() { return length_; }
  public long getOffset() { return offset_; }
  public String getFileName() { return fileName_; }
}
