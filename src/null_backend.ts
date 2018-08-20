// BSD 3-Clause License
//
// Copyright (c) 2018, IBM Corporation
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// * Redistributions of source code must retain the above copyright notice, this
//   list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
//   this list of conditions and the following disclaimer in the documentation
//   and/or other materials provided with the distribution.
//
// * Neither the name of the copyright holder nor the names of its
//   contributors may be used to endorse or promote products derived from
//   this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

import { ResultMessage } from "./messages";
import { ResultBackend } from "./result_backend";

/**
 * A dummy backend. All operations will either throw or return a rejected
 * `Promise`, whichever is appropriate. To be used when ignoring results.
 */
export class NullBackend implements ResultBackend {
    /**
     * @returns A `NullBackend` ready to fail at the earliest convenience.
     */
    public constructor() { }

    /**
     * @returns A rejected `Promise`.
     */
    public put(): Promise<string> {
        return Promise.reject(new Error(
            "cannot put results onto a null backend"
        ));
    }

    /**
     * @returns A rejected `Promise`.
     */
    public get<T>(): Promise<ResultMessage<T>> {
        return Promise.reject(new Error(
            "cannot get results from a null backend"
        ));
    }

    /**
     * @returns A rejected `Promise`.
     */
    public delete(): Promise<string> {
        return Promise.reject(new Error(
            "cannot delete results from a null backend"
        ));
    }

    /**
     * @returns A resolved `Promise`.
     */
    public async disconnect(): Promise<void> {
        return this.end();
    }

    /**
     * @returns A resolved `Promise`.
     */
    public async end(): Promise<void> {
        return Promise.resolve();
    }

    /**
     * @returns Nothing; will never return.
     * @throws Error If invoked.
     */
    public uri(): never {
        throw new Error("cannot query the URI of a null backend");
    }
}
