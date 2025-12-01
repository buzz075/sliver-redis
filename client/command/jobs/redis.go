package jobs

/*
	Sliver Implant Framework
	Copyright (C) 2019  Bishop Fox

	This program is free software: you can redistribute it and/or modify
	it under the terms of the GNU General Public License as published by
	the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	This program is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU General Public License for more details.

	You should have received a copy of the GNU General Public License
	along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

import (
	"context"

	"github.com/bishopfox/sliver/client/console"
	"github.com/bishopfox/sliver/protobuf/clientpb"
	"github.com/spf13/cobra"
)

// RedisListenerCmd - Start a Redis listener.
func RedisListenerCmd(cmd *cobra.Command, con *console.SliverClient, args []string) {
	lhost, _ := cmd.Flags().GetString("lhost")
	lport, _ := cmd.Flags().GetUint32("lport")
	password, _ := cmd.Flags().GetString("password")
	database, _ := cmd.Flags().GetUint32("database")
	tlsEnabled, _ := cmd.Flags().GetBool("tls")

	con.PrintInfof("Starting Redis %s:%d listener ...\n", lhost, lport)
	redis, err := con.Rpc.StartRedisListener(context.Background(), &clientpb.RedisListenerReq{
		Host:       lhost,
		Port:       lport,
		Password:   password,
		DB:         database,
		TLSEnabled: tlsEnabled,
	})
	if err != nil {
		con.PrintErrorf("%s\n", err)
	} else {
		con.PrintInfof("Successfully started job #%d\n", redis.JobID)
	}
}
