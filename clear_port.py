# clear_port.py
import psutil
import argparse
import sys

def find_process_using_port(port):
    """Find the process using the specified port."""
    for conn in psutil.net_connections(kind='inet'):
        if conn.laddr.port == port and conn.status == psutil.CONN_LISTEN:
            return conn.pid
    return None

def kill_process(pid):
    """Terminate the process with the given PID."""
    try:
        process = psutil.Process(pid)
        process.terminate()
        process.wait(timeout=3)  # Wait up to 3 seconds for the process to terminate
        print(f"Successfully terminated process with PID {pid}")
        return True
    except psutil.NoSuchProcess:
        print(f"No process found with PID {pid}")
        return False
    except psutil.TimeoutExpired:
        print(f"Process with PID {pid} did not terminate within 3 seconds, forcing termination...")
        process.kill()
        return True
    except Exception as e:
        print(f"Failed to terminate process with PID {pid}: {e}")
        return False

def clear_port(port):
    """Clear the specified port by terminating the process using it."""
    pid = find_process_using_port(port)
    if pid is None:
        print(f"Port {port} is not in use.")
        return True
    print(f"Port {port} is in use by process with PID {pid}.")
    return kill_process(pid)

def main():
    parser = argparse.ArgumentParser(description="Clear a specific port by terminating the process using it.")
    parser.add_argument("port", type=int, help="The port number to clear (e.g., 5173)")
    args = parser.parse_args()

    if not 0 <= args.port <= 65535:
        print("Error: Port number must be between 0 and 65535.")
        sys.exit(1)

    success = clear_port(args.port)
    if success:
        print(f"Port {args.port} is now free.")
    else:
        print(f"Failed to clear port {args.port}.")
        sys.exit(1)

if __name__ == "__main__":
    main()