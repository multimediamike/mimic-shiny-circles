/*
 *  Copyright (C) 2006 Mike Melanson (mike at multimedia.cx)
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

/*
 * Compact Disc Detection Utility
 *
 * compile with:
 *   gcc -Wall cdinfo2json.c -o cdinfo2json
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <linux/cdrom.h>

#define CD_SECONDS_PER_MINUTE 60
#define CD_FRAMES_PER_SECOND 75
#define CD_RAW_FRAME_SIZE 2352

typedef struct _cdrom_toc_entry
{
  int track_mode;
  int first_frame;
  int first_frame_minute;
  int first_frame_second;
  int first_frame_frame;
  int frame_count;
} cdrom_toc_entry;

typedef struct _cdrom_toc
{
  int first_track;
  int last_track;
  int total_tracks;

  cdrom_toc_entry *toc_entries;
  cdrom_toc_entry leadout_track;
} cdrom_toc;

static void read_cdrom_toc(int fd, cdrom_toc *toc)
{
  struct cdrom_tochdr tochdr;
  struct cdrom_tocentry tocentry;
  int i;

  /* fetch the table of contents */
  if (ioctl(fd, CDROMREADTOCHDR, &tochdr) == -1)
  {
    perror("CDROMREADTOCHDR");
    return;
  }

  toc->first_track = tochdr.cdth_trk0;
  toc->last_track = tochdr.cdth_trk1;
  toc->total_tracks = toc->last_track - toc->first_track + 1;

  /* allocate space for the toc entries */
  toc->toc_entries = 
    (cdrom_toc_entry *)malloc(toc->total_tracks * sizeof(cdrom_toc_entry));
  if (!toc->toc_entries)
  {
    perror("malloc");
    return;
  }

  /* fetch each toc entry */
  for (i = toc->first_track; i <= toc->last_track; i++)
  {
    memset(&tocentry, 0, sizeof(tocentry));

    tocentry.cdte_track = i;
    tocentry.cdte_format = CDROM_MSF;
    if (ioctl(fd, CDROMREADTOCENTRY, &tocentry) == -1)
    {
      perror("CDROMREADTOCENTRY");
      return;
    }

#if 0
printf("TOC entry %d\n", i);
printf("  cdte_track = %d (0x%X)\n", tocentry.cdte_track, tocentry.cdte_track);
printf("  cdte_adr = %d (0x%X)\n", tocentry.cdte_adr, tocentry.cdte_adr);
printf("  cdte_ctrl = %d (0x%X)\n", tocentry.cdte_ctrl, tocentry.cdte_ctrl);
printf("  cdte_format = %d (0x%X)\n", tocentry.cdte_format, tocentry.cdte_format);
printf("  cdte_datamode = %d (0x%X)\n", tocentry.cdte_datamode, tocentry.cdte_datamode);
#endif
    toc->toc_entries[i-1].track_mode = (tocentry.cdte_ctrl & 0x04) ? 1 : 0;
    toc->toc_entries[i-1].first_frame_minute = tocentry.cdte_addr.msf.minute;
    toc->toc_entries[i-1].first_frame_second = tocentry.cdte_addr.msf.second;
    toc->toc_entries[i-1].first_frame_frame = tocentry.cdte_addr.msf.frame;
    toc->toc_entries[i-1].first_frame = 
      (tocentry.cdte_addr.msf.minute * CD_SECONDS_PER_MINUTE * CD_FRAMES_PER_SECOND) +
      (tocentry.cdte_addr.msf.second * CD_FRAMES_PER_SECOND) +
       tocentry.cdte_addr.msf.frame;
#if 0
printf("TOC entry %d\n", i);
printf("  %02d:%02d:%02d\n",
    tocentry.cdte_addr.msf.minute,
    tocentry.cdte_addr.msf.second,
    tocentry.cdte_addr.msf.frame);
printf("  first_frame = %d, mode = %d\n", toc->toc_entries[i-1].first_frame, toc->toc_entries[i-1].track_mode);

    tocentry.cdte_track = i;
    tocentry.cdte_format = CDROM_LBA;
    if (ioctl(fd, CDROMREADTOCENTRY, &tocentry) == -1)
    {
      perror("CDROMREADTOCENTRY");
      return;
    }
    printf("  LBA = %d\n", tocentry.cdte_addr.lba);
#endif

    /* derive the length of the previous frame */
    if (i > 1)
      toc->toc_entries[i-2].frame_count =
        toc->toc_entries[i-1].first_frame - toc->toc_entries[i-2].first_frame;
  }

  /* fetch the leadout as well */
  memset(&tocentry, 0, sizeof(tocentry));

  tocentry.cdte_track = CDROM_LEADOUT;
  tocentry.cdte_format = CDROM_MSF;
  if (ioctl(fd, CDROMREADTOCENTRY, &tocentry) == -1)
  {
    perror("CDROMREADTOCENTRY");
    return;
  }

  toc->leadout_track.track_mode = (tocentry.cdte_ctrl & CDROM_DATA_TRACK) ? 1 : 0;
  toc->leadout_track.first_frame_minute = tocentry.cdte_addr.msf.minute;
  toc->leadout_track.first_frame_second = tocentry.cdte_addr.msf.second;
  toc->leadout_track.first_frame_frame = tocentry.cdte_addr.msf.frame;
  toc->leadout_track.first_frame = 
    (tocentry.cdte_addr.msf.minute * CD_SECONDS_PER_MINUTE * CD_FRAMES_PER_SECOND) +
    (tocentry.cdte_addr.msf.second * CD_FRAMES_PER_SECOND) +
     tocentry.cdte_addr.msf.frame;

  /* derive the length of the final track */
  toc->toc_entries[toc->last_track-1].frame_count =
    toc->leadout_track.first_frame -
      toc->toc_entries[toc->last_track-1].first_frame;
}

static void read_cdrom_frame(int fd, int frame, 
  unsigned char data[CD_RAW_FRAME_SIZE])
{
  struct cdrom_msf msf;

  /* read from starting frame... */
  msf.cdmsf_min0 = frame / CD_SECONDS_PER_MINUTE / CD_FRAMES_PER_SECOND;
  msf.cdmsf_sec0 = (frame / CD_FRAMES_PER_SECOND) % CD_SECONDS_PER_MINUTE;
  msf.cdmsf_frame0 = frame % CD_FRAMES_PER_SECOND;

  /* read until ending track (starting frame + 1)... */
  msf.cdmsf_min1 = (frame + 1) / CD_SECONDS_PER_MINUTE / CD_FRAMES_PER_SECOND;
  msf.cdmsf_sec1 = ((frame + 1) / CD_FRAMES_PER_SECOND) % CD_SECONDS_PER_MINUTE;
  msf.cdmsf_frame1 = (frame + 1) % CD_FRAMES_PER_SECOND;

  /* MSF structure is the input to the ioctl */
  memcpy(data, &msf, sizeof(msf));

  /* read a frame */
  if(ioctl(fd, CDROMREADRAW, data, data) < 0) {
    perror("CDROMREADRAW");
    return;
  }
}

int main(int argc, char *argv[])
{
  int fd;
  int status;
  cdrom_toc toc;
  unsigned char frame[CD_RAW_FRAME_SIZE];
  int i, j;
  int iso_start_index;

  if (argc < 2) {
    printf ("Usage: cdinfo2json </path/to/cdrom/device>\n\n");
    return 0;
  }

  /* open CD-ROM */
  fd = open(argv[1], O_RDONLY | O_NONBLOCK);
  if (fd == -1) {
    perror(argv[1]);
    return 1;
  }

  read_cdrom_toc(fd, &toc);
  printf("{\n  \"track_count\": %d,\n  \"tracks\":\n  [\n",
    toc.total_tracks);

  for (i = toc.first_track; i <= toc.last_track; i++)
  {
    printf("    {\n      \"track_type\": \"%s\",\n",
      (toc.toc_entries[i-1].track_mode == 0) ? "audio" : "data");
    printf("      \"first_sector\": %d,\n",
      toc.toc_entries[i-1].first_frame);
    printf("      \"sector_count\": %d",
      toc.toc_entries[i-1].frame_count);

    /* if this is a data track, get the 16th frame and look for data mode
     * and iso9660 signature ('CD001') */
    if (toc.toc_entries[i-1].track_mode == 1)
    {
      printf(",\n");
      read_cdrom_frame(fd, toc.toc_entries[i-1].first_frame+16, frame);

      if (frame[0x0F] == 1) {
        printf ("      \"data_type\": \"mode 1\"");
        iso_start_index = 0x10;
      } else if (frame[0x0F]) {
        if (frame[0x12] & 0x20) {
            printf ("      \"data_type\": \"mode 2/form 2\"");
        } else {
            printf ("      \"data_type\": \"mode 2/form 1\"");
        }
        iso_start_index = 0x18;
      }

      if ((frame[iso_start_index + 1] == 'C') &&
          (frame[iso_start_index + 2] == 'D') &&
          (frame[iso_start_index + 3] == '0') &&
          (frame[iso_start_index + 4] == '0') &&
          (frame[iso_start_index + 5] == '1')) {

//        printf (" iso9660 fs signature found\n");
//        printf (" system id = ");
        for (j = 8; j < 40; j++) {
//          printf ("%c", frame[iso_start_index + j]);
        }
//        printf ("\n");

//        printf (" volume id = ");
        for (j = 40; j < 72; j++) {
//          printf ("%c", frame[iso_start_index + j]);
        }
//        printf ("\n");
      }
    }
    printf("\n    }%s\n",
      (i < toc.last_track) ? "," : " ");
  }

  printf("  ]\n}\n");

  /* close CD-ROM */
  status = close(fd);
  if (status != 0)
    printf ("close() returned status %d\n", status);

  return 0;
}

