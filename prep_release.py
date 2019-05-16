#!/usr/bin/env python
"""
PRovides helper methods to determine the proper version for the current HEAD of
the git repository and ensure that an inconsistent or otherwise non-release
version is not accidentally released.
"""

import re
import tokio
import subprocess

REX_VERS = re.compile("^(\d+)\.(\d+)\.(\d+)([ab]\d+)?$")
REMOTE = 'gkl'

def version2tag():
    """Converts the tokio.__version__ string to a git tag"""
    vers = tokio.__version__.lstrip('v')
    vers_match = REX_VERS.match(vers)
    if vers_match:
        tag = "v%s.%s.%s" % (vers_match.group(1), vers_match.group(2), vers_match.group(3))
        if vers_match.group(4):
            tag += vers_match.group(4).replace('a', '-alpha').replace('b', '-beta')
        return tag
    raise RuntimeError("version %s does not look release-ready" % vers)

def git_version():
    """Return components of the version according to git

    Returns a tuple of type (str, str, str) describing 
    
    1. the next major release in X.Y.Z format
    2. any prerelease suffixes (-alphaA or -betaA), and 
    3. any extras describing the dev version (.devN) and dirtiness of the
       current working directory (+dirty).

    If the second and third tuple members are empty strings, this is a clean and
    tagged version ready for release.

    Returns:
        tuple: (str, str, str) describing the next major release, prerelease
        suffixes, and extras
    """
    # get the closest tag to HEAD
    tag = subprocess.check_output(['git', 'describe', '--tags', '--dirty']).decode("utf-8").strip()

    # split the target release version from any prerelease decorators
    release_vers, extras = tag.split('-', 1)
    extras = extras

    # if HEAD is not tagged, this is a dev release
    dirty = False
    suffix = ""
    prerelease = ""

    # 0.12.0-dirty
    # 0.12.0-beta4
    # 0.12.0-beta4-dirty
    # 0.12.0-beta4-5-3d9ab49
    # 0.12.0-beta4-5-3d9ab49-dirty
    if extras.endswith('dirty'):
        dirty = True
        extras = extras.rsplit("-", 1)[0]

    if len(extras) >= 4 and extras[:4] in ('alph', 'beta'):
        if '-' in extras:
            prerelease, extras = extras.split('-', 1)
        else:
            prerelease = extras
            extras = ""

    if prerelease:
        prerelease = "-" + prerelease

    if '-' in extras:
        suffix = ".dev%d" % int(extras.split('-', 1)[0])

    if dirty:
        suffix += "+dirty"

    return release_vers, prerelease, suffix

class ReleaseInfo():
    def __init__(self, remote):
        self.remote = remote
    
        self.release_vers, self.prerelease, self.suffix = git_version()
        self.tagname = version2tag()
        self.full_vers = "%s%s%s" % (self.release_vers, self.prerelease, self.suffix)

    def is_remote_tag(self):
        """Determines if a tag exists on a remote

        Returns:
            bool: True if tag exists upstream
        """
        expected_tag = "refs/tags/%s" % self.tagname
        for line in subprocess.check_output(['git', 'ls-remote', '--tags', self.remote]).splitlines():
            remote_tag = line.split(None, 1)[-1].decode("utf-8").strip()
            if expected_tag == remote_tag:
                return True

        return False

    def is_clean(self):
        """Determines if HEAD is tagged and ready to release
        """
        return not self.suffix

    def __repr__(self):
        ret = ""
        ret += "Base version from git: %s\n" % self.release_vers
        ret += "Prerelease from git:   %s\n" % self.prerelease
        ret += "Suffix from git:       %s\n" % self.suffix
        ret += "Expecting tag:         %s\n" % self.tagname
        ret += "Full version from git: %s\n" % self.full_vers
        return ret

def main(argv=None):
    releaseinfo = ReleaseInfo(REMOTE)

    print(releaseinfo)
    print("Version from tokio:    %s" % tokio.__version__)
    print("Tag exists upstream?   %s" % releaseinfo.is_remote_tag())

    print("Are we ready to release?")
    clean_tag = releaseinfo.is_clean()
    exists_upstream = releaseinfo.is_remote_tag()
    print("  Is this a clean tag? %s" % clean_tag)
    print("  Is tag new upstream? %s" % (not exists_upstream))

    if clean_tag and not exists_upstream:
        print("")
        print("Ready to do:")
        print("  1. git push --tags")
        print("  2. Build ")

if __name__ == "__main__":
    main()
