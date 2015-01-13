/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

class Cosa{

    public Cosa(String campo1, int campo2){
        this.campo1 = campo1;
        this.campo2 = campo2;
    }
    public String campo1;

    public int campo2;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Cosa cosa = (Cosa) o;

        if (campo1 != null ? !campo1.equals(cosa.campo1) : cosa.campo1 != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return campo1 != null ? campo1.hashCode() : 0;
    }
}
