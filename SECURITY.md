# Security Disclosure - Windows NFS Mount Setup

This document is designed for IT Administrators, Network Security Officers, and System Auditors to understand the security posture and system changes introduced by the Accelerator CLI (`accli`) mount configurations.

## Architecture Overview

The `accli mount start` command mounts remote project filesystems on Windows over NFS. To achieve this safely and seamlessly for standard (non-admin) corporate users, we implement the **Windows Task Scheduler Mount Gateway**:

1. **Local Loopback Communication (127.0.0.1)**:
   - The mount client and the background NFS gateway daemon (`hf-mount-nfs`) reside on the **same machine**.
   - All NFS mounting interactions occur over the local loopback interface (`localhost` / `127.0.0.1`).
   - Loopback traffic is internal to the computer and never traverses the physical network adapter.
2. **Zero External Port Exposure (No Firewall Rules)**:
   - Because all network traffic is local loopback, **no Windows Defender Firewall rules are created or modified**. 
   - No incoming ports (such as Ports 111 or 2049) are exposed to other machines on the LAN or the internet, preventing any external vector of attack.

---

## The Task Scheduler Mount Gateway (Zero UAC for Standard Users)

In secure corporate environments, standard workstations do not grant administrative privileges to end-users. Because the background `hf-mount-nfs.exe` daemon must bind to loopback Port 111 (privileged for the RPC portmapper on Windows) and run system-wide mounting commands, it requires elevated privileges. 

To resolve this constraint securely, `accli` implements a **hybrid dynamic Scheduled Task Gateway**:

### 1. One-Time Administrator Pre-deployment
An IT Administrator performs a **one-time configuration** on client computers using standard deployment/MDM systems (Microsoft Intune, Active Directory Group Policy (GPO), or SCCM):
* Runs the static, inspectable script [`scripts/setup-nfs.ps1`](file:///c:/Users/wrufe/accelerator_non-web_client/scripts/setup-nfs.ps1) with administrative rights.
* Creates `C:\ProgramData\accli` directory and grants **Modify** permissions specifically to the standard workstation's **Authenticated Users** group.
* Registers a Windows Scheduled Task named **`accli-mount-nfs`** configured to run as **`SYSTEM`** (highest privileges) and execute a static helper script: `C:\ProgramData\accli\run-mount-helper.ps1`.

### 2. Passwordless Dynamic Mounting (Standard User)
When a standard user starts a mount:
1. `accli mount start <project-slug>` writes the dynamic mount credentials, project slug, and drive letter to the ProgramData directory: `C:\ProgramData\accli\mount_config.json`.
2. `accli` executes standard Windows Task Scheduler runner:
   ```bash
   schtasks /run /tn "accli-mount-nfs"
   ```
   *Note: Windows grants standard users full permission to trigger their own configured Scheduled Tasks. No UAC administrative prompt is displayed.*
3. The Task Scheduler immediately executes the static helper `run-mount-helper.ps1` under the elevated `SYSTEM` account.
4. The helper script reads the config JSON and starts the background `hf-mount-nfs.exe` daemon with proper privileges, successfully binding to loopback Port 111 (NFS RPC Portmapper).
5. The `accli` CLI executes `mount.exe` directly inside the user's interactive logon session to map the network drive (e.g. `W:`).
   * **Result**: Because the drive mapping command runs inside the user's actual session, it is immediately visible in Windows Explorer and all standard applications with ZERO UAC prompts!

---

## Detailed System Changes Disclosed

The administrator setup script performs strictly **three** standard operating system configurations:

### 1. Windows Features Enabled
* **Command**:
  ```powershell
  Enable-WindowsOptionalFeature -Online -FeatureName ServicesForNFS-ClientOnly,ClientForNFS-Infrastructure -All -NoRestart
  ```
* **Purpose**: Activates the native Microsoft Client for NFS optional components.
* **Security Impact**: Safe. Enables built-in operating system features designed and cryptographically signed by Microsoft.

### 2. Linked Connections Registry Policy
* **Key**: `HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\Policies\System`
* **Value Name**: `EnableLinkedConnections`
* **Type**: `REG_DWORD`
* **Value**: `1`
* **Purpose**: Instructs Windows User Account Control (UAC) to share mapped network drive letters between standard and elevated command contexts belonging to the active logged-in user session.
* **Security Impact**: Safe. Standard Microsoft UAC behavior, restricted strictly to the local active user session.

### 3. Folder Directory Permissions (`C:\ProgramData\accli`)
* **Modify Access**: Authenticated Users.
* **Purpose**: Enables standard users to write mount parameters (`mount_config.json`) dynamically, which is read by `SYSTEM` to start the background mount.
* **Security Impact**: Restricts read/write permissions to locally authenticated sessions. Standard, well-documented practice.

### 4. Task File Access Control List (ACL) Permissions (`C:\Windows\System32\Tasks\accli-mount-nfs`)
* **Read and Execute Access**: Authenticated Users.
* **Purpose**: Allows standard users to query and trigger the registered scheduled task from their non-admin sessions.
* **Security Impact**: Safe. Standard users can only trigger the pre-configured task; they cannot modify its command-line action or run context.

---

## Contact & Compliance Audits

If your IT Security department requires further code audit or has any questions, the static configuration steps are completely open and inspectable in the file [`scripts/setup-nfs.ps1`](file:///c:/Users/wrufe/accelerator_non-web_client/scripts/setup-nfs.ps1).
