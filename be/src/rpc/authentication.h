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


#ifndef IMPALA_SERVICE_AUTHENTICATION_H
#define IMPALA_SERVICE_AUTHENTICATION_H

#include <string>
#include <thrift/transport/TTransport.h>
#include <boost/unordered_map.hpp>

#include "common/status.h"
#include "rpc/auth-provider.h"
#include "sasl/sasl.h"
#include "transport/TSaslServerTransport.h"
#include "transport/TSasl.h"
#include "util/locks.h"

using namespace ::apache::thrift::transport;

namespace impala {

/// System-wide authentication manager responsible for initialising authentication systems,
/// including Sasl and Kerberos, and for providing auth-enabled Thrift structures to
/// servers and clients.
class AuthManager {
 public:
  static AuthManager* GetInstance() { return AuthManager::auth_manager_; };

  /// Set up internal and external AuthProvider classes.  This does a bunch of flag
  /// checking and calls each AuthProvider->Start().
  Status Init(bool is_recordservice = false);

  /// Returns the authentication provider to use for "external" communication
  /// such as the impala shell, jdbc, odbc, etc. This only applies to the server
  /// side of a connection; the client side of said connection is never an
  /// internal process.
  AuthProvider* GetExternalAuthProvider();

  /// Returns the authentication provider to use for internal daemon <-> daemon
  /// connections.  This goes for both the client and server sides.  An example
  /// connection this applies to would be backend <-> statestore.
  AuthProvider* GetInternalAuthProvider();

  // Returns the unsecure auth provider.
  AuthProvider* GetNoAuthProvider();

  // SASL mechanisms that we support.
  static const std::string KERBEROS_MECHANISM;
  static const std::string DIGEST_MECHANISM;
  static const std::string PLAIN_MECHANISM;

  // Add/Removes conn as an active digest-md5 connection. Thread safe.
  void AddDigestMd5Connection(sasl_conn_t* conn);
  void RemoveDigestMd5Connection(sasl_conn_t* conn);

  // Sets the user for this DIGEST-MD5 connection. The user in 'conn' is the
  // encoded token (we need the user name for authorization).
  void SetDigestMd5ConnectedUser(sasl_conn_t* conn, const std::string& user);

  // Returns the connected user if conn is an authenticated DIGEST-MD5 connection.
  // return empty string otherwise.
  const std::string& GetDigestMd5ConnectedUser(sasl_conn_t* conn);

  // Returns true if conn is a digest-md5 connection.
  bool IsDigestConnection(const sasl_conn_t* conn);

 private:
  static AuthManager* auth_manager_;

  /// These are provided for convenience, so that demon<->demon and client<->demon services
  /// don't have to check the auth flags to figure out which auth provider to use.
  boost::scoped_ptr<AuthProvider> internal_auth_provider_;
  boost::scoped_ptr<AuthProvider> external_auth_provider_;
  boost::scoped_ptr<AuthProvider> no_auth_provider_;

  // Lock and map of all active server digest md5 connections and their user names.
  // When the connection is created, it is added to this with an empty user.
  // During the token negotiation, the user is set.
  // TODO: it would be ideal if we could just attach this information to the sasl_conn_t
  // object but not sure that is possible.
  SpinLock digest_connection_lock_;
  boost::unordered_map<sasl_conn_t*, std::string> digest_connections_;
};


}
#endif
