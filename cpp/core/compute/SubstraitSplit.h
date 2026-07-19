/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <utility>

#include "substrait/algebra.pb.h"

namespace gluten {

struct SubstraitSplit {
  enum class Kind {
    kLocalFiles,
    kStreamKafka,
  };

  static SubstraitSplit makeLocalFiles(::substrait::ReadRel_LocalFiles split) {
    SubstraitSplit payload;
    payload.kind = Kind::kLocalFiles;
    payload.localFiles = std::move(split);
    return payload;
  }

  static SubstraitSplit makeStreamKafka(::substrait::ReadRel_StreamKafka split) {
    SubstraitSplit payload;
    payload.kind = Kind::kStreamKafka;
    payload.streamKafka = std::move(split);
    return payload;
  }

  Kind kind{Kind::kLocalFiles};
  ::substrait::ReadRel_LocalFiles localFiles;
  ::substrait::ReadRel_StreamKafka streamKafka;
};

} // namespace gluten
