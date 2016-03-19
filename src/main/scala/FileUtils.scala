// Copyright (C) 2016-Present by George K Thiruvathukal.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
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

package cs.luc.edu

import java.io._
import java.nio.file._

package object fileutils {

  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }

  def getFileList(path: String, ext: String): Array[String] = {
    require { ext.startsWith(".") }
    val fullPath = new File(path).getAbsolutePath
    recursiveListFiles(new File(fullPath)).filter(_.getName.endsWith(ext)).map(_.getAbsolutePath)
  }

}
